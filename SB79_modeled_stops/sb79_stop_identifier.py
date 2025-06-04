"""
Name: sb79_stop_identifier.py
Purpose: Very similar to AB2097_modeled_stops, except:
    -does not include all rail stops; just rail stops with >24 trains/day
    -adds "tier" flag

    IDEA 6/4/2025 - could parts of this script be combined with AB2097 script to have "master major stops" tool,
    with separate fields for SB79 AB2097 etc? Or is better to keep them separate.


Author: Darren Conly
Last Updated: June 2025
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""
from pathlib import Path
import datetime as dt

import arcpy
arcpy.env.overwriteOutput = True
from arcgis.features import GeoAccessor, GeoSeriesAccessor

import pandas as pd
pd.options.mode.chained_assignment = None # suppresses "settingwithcopy warning"

from trantxt2linknode_gis import LinesNodes
from netpyconvert import netpyconvert as npc

def esri_object_to_df(in_esri_obj, esri_obj_fields, index_field=None):
    '''converts esri gdb table, feature class, feature layer, or SHP to pandas dataframe'''
    data_rows = []
    with arcpy.da.SearchCursor(in_esri_obj, esri_obj_fields) as cur:
        for row in cur:
            out_row = list(row)
            data_rows.append(out_row)

    out_df = pd.DataFrame(data_rows, index=index_field, columns=esri_obj_fields)
    return out_df

class HQTransitStops:
    def __init__(self, run_folder, scen_yr, keep_all, addstops_csv=None):
        # Parse transit line file, split into node table and line table, as dataframe
        self.keep_all = keep_all # if false, only keep nodes that are HQ transit nodes
        self.scen_yr = scen_yr
        self.in_tranline_txt = Path(run_folder).joinpath(f"{scen_yr}_tranline.lin")
        self.hwy_net = Path(run_folder).joinpath(f"{sc_yr}_base.net")
        self.addstops_csv = addstops_csv

        # load transit data
        self.tranline_data = LinesNodes(self.in_tranline_txt)
        self.line_data = self.tranline_data.line_rows_dict
        self.node_data = self.tranline_data.node_rows
        self.fnames_node_data = self.tranline_data.node_attrs_out_order

        # {headway field: headway period duration}
        self.headway_info = {self.tranline_data.headway1: 240, self.tranline_data.headway2: 360,
                            self.tranline_data.headway3: 180, self.tranline_data.headway4: 120,
                            self.tranline_data.headway5: 240}
        self.headway_cols = list(self.headway_info.keys())

        self.f_color = self.tranline_data.color
        self.color_commrail = 6
        self.mode_rail = 1
        self.dir_tags = ['_A', '_B']
        self.f_name_nodir = 'LNAME_NODIR'
        self.f_corrname = 'LN_CORR_NAME'
        self.f_buslane = 'BUSLANE'
        self.f_nodeid = self.tranline_data.f_node_attrname

        # routes that are coded as express buses, but that are actually rail and thus
        #their stops are considered hi-quality.
        self.rail_exceptions = ['AMTRCC_A', 'AMTRCC_B', 'AMTRCCS_A', 'AMTRCCS_B',
                                'ZF_AMTRCCR_A', 'ZF_AMTRCCR_B', 'ZF_AMTRJPA_A', 'ZF_AMTRJPA_B']

        self.sr_sacog = 2226 # SACOG 4-digit spatial ref code
        self.hifreq_ix_th = 20.9 # max headway in mins to qualify as hi-freq for purposes of identifying hi-freq intersections
        self.hifreq_brt_th = 15.9 # max headway in mins to qualify as BRT, which requires both bus lanes and max headway threshold

    def strip_dirtag(self, in_str):
        out_str = in_str
        if in_str[-2:] in self.dir_tags:
            out_str = in_str[:-2]
        return out_str
    

    def get_daily_trips(self, in_df):

        self.f_daytrips = 'day_trips'
        in_df[self.f_daytrips] = 0.0

        for fname, durn in self.headway_info.items():
            in_df.loc[in_df[fname] > 0, self.f_daytrips] += durn / in_df[fname]


    def create_line_df(self):
        # for line table, only keep fields for line name, mode, color, headway1, headway3

        linecols = [self.tranline_data.f_linename, self.tranline_data.mode,
                    self.tranline_data.color, *self.headway_cols]

        df_lines = pd.DataFrame(self.line_data)[linecols]
        
        self.get_daily_trips(df_lines)

        # Add field to line df that strips direction tag (_A, _B) from line name
        df_lines[self.f_name_nodir] = df_lines[self.tranline_data.f_linename] \
                                        .apply(lambda x: self.strip_dirtag(x))

        # Create consolidated lines df that is only unique lines after having direction stripped
        aggdict = {hwcol: 'min' for hwcol in self.headway_cols}
        aggdict.update({self.f_daytrips: 'sum'})
        gbcols = [self.f_name_nodir, self.tranline_data.mode, self.tranline_data.color]
        df_lines_out = df_lines.groupby(gbcols).agg(aggdict).reset_index()

        return df_lines_out
        

    def create_stops_df(self):

        node_cols = [self.tranline_data.f_linename, self.tranline_data.f_node_attrname,
                    self.tranline_data.stopflag]
        df_nodes_raw = pd.DataFrame(self.node_data, columns=self.fnames_node_data)[node_cols]
        df_nodes_raw[self.f_nodeid] = df_nodes_raw[self.f_nodeid].astype('int') # convert stop node ID to int data type

        # Filter stops df to only include stop nodes
        self.df_nodes = df_nodes_raw.loc[df_nodes_raw[self.tranline_data.stopflag] == self.tranline_data.val_stop]

        # Add field to stop df that strips direction tag (_A, _B) from line name
        self.df_nodes[self.f_name_nodir] = self.df_nodes[self.tranline_data.f_linename] \
                                        .apply(lambda x: self.strip_dirtag(x))

        cols_nodir = [c for c in self.df_nodes.columns if c != self.tranline_data.f_linename]
        df_nodes_nodir = self.df_nodes[cols_nodir].drop_duplicates()

        return df_nodes_nodir

    def get_node_svc_data(self, df_lnd_data, node_id):
        # for a given node, identify all services that qualify it as a major node

        # For each node ID:
        f_node = self.tranline_data.f_node_attrname
        f_mode = self.tranline_data.mode
        f_name = self.f_name_nodir

        self.rail_exceptions_trimmed = [self.strip_dirtag(n) for n in self.rail_exceptions]

        
        # df_node = filter dfjoin to only have rows with node ID
        df_node = df_lnd_data.loc[df_lnd_data[f_node] == node_id]

        df_node_anyrail = df_node.loc[(df_node[f_mode] == self.mode_rail) \
                            | (df_node[f_name].isin(self.rail_exceptions_trimmed))]
        
        # df_node_lrt = lrt stations only (MODE=1)
        df_node_lrt = df_node.loc[(df_node[f_mode] == self.mode_rail)]


        # Find rows that count as BRT major stops where both served by bus-only lane and where headway is meets frequency threshold
        df_node_brt = df_node.loc[(df_node[f_mode] != self.mode_rail) # non-rail 
                                    & (~df_node[f_name].isin(self.rail_exceptions_trimmed)) # non-rail-exception
                                    & (df_node[self.tranline_data.headway3] > 0) & (df_node[self.tranline_data.headway3] <= self.hifreq_brt_th) # hi-freq in AM
                                    & (df_node[self.tranline_data.headway1] > 0) & (df_node[self.tranline_data.headway1] <= self.hifreq_brt_th)  # hi_freq in PM
                                    & (df_node[self.f_buslane] > 0) # served by buslanes
                                ]
        
        # get number of bidirectional total daily commuter trips
        # must agg by corridor flag if 2 comm rail routes can have trips combined (e.g., short v long versions of cap corridor)
        commrail_trips = 0
        commrail_stns = df_node[df_node[self.f_color] == self.color_commrail]
        if commrail_stns.shape[0] > 0:
            commrail_trips = commrail_stns.groupby(self.f_corrname)[self.f_daytrips].max().values[0]

        all_lines = ';'.join(df_node[f_name]) if df_node.shape[0] > 0 else ''
        rail_lines = ';'.join(df_node_anyrail[f_name]) if df_node_anyrail.shape[0] > 0 else ''
        lrt_lines = ';'.join(df_node_lrt[f_name]) if df_node_lrt.shape[0] > 0 else ''
        brt_lines = ';'.join(df_node_brt[f_name]) if df_node_brt.shape[0] > 0 else ''


        out_data = {self.tranline_data.f_node_attrname: node_id, 
            self.k_all_lines: all_lines, self.k_rail_lines: rail_lines, self.k_lrt_lines: lrt_lines, 
            self.k_brt_lines: brt_lines, self.k_commrail_trips: commrail_trips}

        return out_data

    
    def get_buslane_info(self, transit_node_df, link_qual_threshold=1):
        # tag each node in transit_node_df with 1/0 flag indicating if served 
        # by bus-only lanes; link_qual_threshold determins if it counts based off being the end of a bus lane
        # vs if it needs to have at least 2 links leaving it to count as being served by bus-only lane
        
        # hwy fields
        self.f_a = 'A'
        self.f_b = 'B'
        all_link_fields = [self.f_a, self.f_b, self.f_buslane]

        # convert hwy NET file to link DBF
        hwy_link_dbf = npc.net2dbf(self.hwy_net, scenario_prefix=sc_yr, geom_type="LINK", skip_if_exists=True)

        # load link DBF to DF
        link_dbf_fields = [f.name for f in arcpy.ListFields(hwy_link_dbf)]
        linkfields = [f for f in all_link_fields if f in link_dbf_fields]
        hwy_link_df = esri_object_to_df(in_esri_obj=hwy_link_dbf, esri_obj_fields=linkfields)
        for f in all_link_fields:
            if f not in hwy_link_df.columns: 
                print(f"\tfield {f} not in hwy network, so adding and setting=0...")
                hwy_link_df[f] = 0 # make sure all needed fields added, but set to zero if they aren't in hwy net 
        hwy_link_df = hwy_link_df.loc[hwy_link_df[self.f_buslane] > 0] # only want links with bus lanes

        buslane_a = hwy_link_df[self.f_a].value_counts().reset_index().rename(columns={self.f_a: self.f_nodeid})
        buslane_b = hwy_link_df[self.f_b].value_counts().reset_index().rename(columns={self.f_b: self.f_nodeid})
        buslnstops = pd.concat([buslane_a, buslane_b]).drop_duplicates()
        buslnstops = buslnstops.loc[buslnstops['count'] >= link_qual_threshold]

        dfjn = transit_node_df.merge(buslnstops, how='left', on=self.f_nodeid).fillna(0) \
                .rename(columns={'count': self.f_buslane})
        dfjn.loc[dfjn[self.f_buslane] > 0, self.f_buslane] = 1 # set to 1/0 flag for whether there is a buslane

        return dfjn

    def make_hq_stop_df(self):
        
        arcpy.AddMessage("Filtering and identifying high-quality transit stops...")
        df_hq_lines = self.create_line_df()
        df_stopnodes = self.create_stops_df()

        # unique stop nodes
        stopnode_list = list(df_stopnodes[self.tranline_data.f_node_attrname].unique())
        
        self.k_all_lines = "all_lines"
        self.k_rail_lines = 'all_rail_lines'
        self.k_lrt_lines = "lrt_lines"
        self.k_brt_lines = "brt_lines"
        self.k_commrail_trips = 'crail_bidir_trps'
        self.k_majstoptyp = 'maj_stop'

        # tags
        self.not_maj = 'Non-major stop'
        self.maj_rail = 'Rail'
        self.maj_brt = 'BRT'
        self.maj_ix = '2+ Hi-frequency bus routes'

        # Join consolidated lines df to stop nodes df (inner join) on stripped line name.
        self.df_linenode_joined = df_stopnodes.merge(df_hq_lines, on=self.f_name_nodir)

        # tag whether each node is served by bus-only lanes
        self.df_linenode_joined = self.get_buslane_info(self.df_linenode_joined)

        self.add_corridor_lname(self.df_linenode_joined)

        node_svc_data = []
        for node in stopnode_list:
            node_data = self.get_node_svc_data(self.df_linenode_joined, node)
            node_svc_data.append(node_data)

        self.final_df = pd.DataFrame(node_svc_data)
        self.final_df[self.tranline_data.f_node_attrname] = self.final_df[self.tranline_data.f_node_attrname].astype('int')

        # consolidate to single field that identifies what type of major stop it is
        self.final_df[self.k_majstoptyp] = self.not_maj # default value: not a major transit stop
        self.final_df.loc[~self.final_df[self.k_rail_lines].isin(["", " "]), self.k_majstoptyp] = self.maj_rail
        self.final_df.loc[~self.final_df[self.k_brt_lines].isin(["", " "]), self.k_majstoptyp] = self.maj_brt

        if not self.keep_all:
            # option to exclude non-major stops from output
            self.final_df = self.final_df.loc[(self.final_df[self.k_majstoptyp] != 'Non-major stop')]

        self.final_df = self.add_spatial_data(self.final_df)

        self.final_df = self.add_urb_county_tag(self.final_df)

        self.f_tier = 'sb79tier'
        self.final_df[self.f_tier] = 0 # by default, no SB79 nodes
        self.final_df[self.f_tier] = self.final_df.apply(lambda x: self.get_sb79_tier(x), axis=1)
        
        return self.final_df
    
    def get_sb79_tier(self, node_data):

        lrt_stop = node_data[self.k_lrt_lines] != ''
        brt_stop = node_data[self.k_brt_lines] != ''
        hfreq_commrail = node_data[self.k_commrail_trips] >= 48
        freq_commrail = node_data[self.k_commrail_trips] >= 24

        # no tier 1 nodes in SACOG region

        tier = 0

        if any([lrt_stop, brt_stop, hfreq_commrail, freq_commrail]):
            """Tier 3 = a transit-oriented development stop within an urban transit county not already covered by Tier 2 criteria, 
            served by frequent commuter rail service; or any transit-oriented development stop not within an urban transit county; 
            or any major transit stop otherwise so designated by the applicable authority."""
            tier = 3
        
        if node_data[self.f_urb_county] == 1 \
            and any([lrt_stop, brt_stop, hfreq_commrail]):
            """Tier 2 = stop within an urban transit county, excluding a Tier 1 transit-oriented development stop, 
            served by light rail transit, by high-frequency commuter rail, 
            or by bus service with headways <15mins and dedicated lanes. 
            """
            tier = 2
        
        return tier



    
    def add_corridor_lname(self, in_df):
        # tag if 2+ lines can have their trips combined for purposes of determining commuter route frequency.
        # e.g., AMTRCC and AMTRCCS can sum their trips together if there's total overlap, e.g., between SVS and Davis

        in_df[self.f_corrname] = in_df[self.f_name_nodir]

        lnames = in_df[self.f_name_nodir].drop_duplicates()
        for lname in lnames:
            lnodes = in_df[in_df[self.f_name_nodir] == lname][self.f_nodeid].values

            lnames2 = in_df[in_df[self.f_name_nodir] != lname][self.f_name_nodir].drop_duplicates()
            for lname2 in lnames2:
                lnodes2 = in_df[in_df[self.f_name_nodir] == lname2][self.f_nodeid].values
                l1_in_l2 = all([i in lnodes2 for i in lnodes]) # True if all nodes in l1 are in l2
                l2_in_l1 = all([i in lnodes for i in lnodes2]) # True if all in l2 are in l1
                
                if l1_in_l2:
                    # if all line 1's nodes within line 2, then the corridor name will be line 1
                    corr_nodes = in_df[in_df[self.f_name_nodir] == lname][self.f_nodeid].values
                    in_df.loc[(in_df[self.f_name_nodir].isin([lname, lname2])) \
                              & (in_df[self.f_nodeid].isin(corr_nodes)), self.f_corrname] = lname
                elif l2_in_l1:
                    # if all line 2's nodes within line 1, then the corridor name will be line 2
                    corr_nodes = in_df[in_df[self.f_name_nodir] == lname2][self.f_nodeid].values
                    in_df.loc[in_df[self.f_nodeid].isin(corr_nodes), self.f_corrname] = lname2
                else:
                    # if not total overlap in either way, then skip
                    continue


    
    def manual_stop_add(self, sedf_in, stops_csv, csv_crs_id=4326):
        # manually add stops that are not in SACSIM stop network (e.g., Colfax train station)
        dfstops = pd.read_csv(stops_csv)
        sdf_stops = pd.DataFrame.spatial.from_xy(dfstops, x_column='x', y_column='y', sr=csv_crs_id)
        sdf_stops.spatial.project(self.sr_sacog)
        sdf_stops = sdf_stops[[f for f in sdf_stops.columns if f in sedf_in.columns]]

        df_out = pd.concat([sedf_in, sdf_stops])

        return df_out
    
    def add_spatial_data(self, in_df):
        """Join table of high-qual transit stops to X/Y data for each node from
        Cube hwy net"""

        node_x = 'X'
        node_y = 'Y'
        hwy_node_fields = [self.f_nodeid, node_x, node_y]
        hwy_node_dbf = npc.net2dbf(self.hwy_net, scenario_prefix=self.scen_yr, skip_if_exists=True)
        df_hwynodes = esri_object_to_df(in_esri_obj=hwy_node_dbf, esri_obj_fields=hwy_node_fields)
        
        sedf_join = df_hwynodes.merge(in_df, on=self.tranline_data.f_node_attrname)
        sedf_out = pd.DataFrame.spatial.from_xy(sedf_join, x_column=node_x, y_column=node_y, sr=self.sr_sacog)

        if self.addstops_csv: 
            sedf_out = self.manual_stop_add(sedf_out, self.addstops_csv)

        return sedf_out
    
    def add_urb_county_tag(self, sedf):
        # "urban transit counties", as of 6/4/2025, are those with 15 or more rail stations in them
        fc_counties = r'I:\Projects\Darren\2025BlueprintTables\Blueprint_Table_GIS\winuser@GISData.sde\GISOWNER.AdministrativeBoundaries\GISOWNER.Counties'
        f_cname = 'COUNTY'
        
        self.f_urb_county = 'sb79_urbcnty'
        sedf[self.f_urb_county] = 0 # by default, stop is not in "urban transit county"

        # identify which counties qualify as "urban transit counties"
        sedf_counties = pd.DataFrame.spatial.from_featureclass(fc_counties)[['SHAPE', f_cname]]
        sedf = sedf.spatial.join(sedf_counties)

        railstop_x_county = sedf[sedf[self.k_lrt_lines] != ''].groupby(f_cname)[self.k_lrt_lines].count().to_dict()
        urb_counties = [county for county in railstop_x_county.keys() if railstop_x_county[county] > 15]

        # tag which stops are in urban transit county
        sedf.loc[sedf[f_cname].isin(urb_counties), self.f_urb_county] = 1

        return sedf

    def export_to_esri_fc(self, sedf, output_gdb):
        """Join table of high-qual transit stops to X/Y data for each node from
        Cube hwy net, then export to SHP"""

        self.scen = Path(self.in_tranline_txt).stem
        self.tsufx = str(dt.datetime.now().strftime('%Y%m%d_%H%M'))
        out_fc = f"SB79Stops_{self.scen}{self.tsufx}"
        out_path = str(Path(output_gdb).joinpath(out_fc))
        sedf.spatial.to_featureclass(out_path, sanitize_columns=False)

        user_msg = f"""Success! Output file is {out_path}"""
        arcpy.AddMessage(user_msg)

        return out_path

    def export_to_csv(self, output_dir):
        out_csv = f"SB79Stops_{self.scen}{self.tsufx}"        
        self.final_df.to_csv(Path(output_dir).joinpath(out_csv), index=False)


if __name__ == '__main__':
    # primary inputs
    model_run_dir = input("Enter model run folder path: ").strip("\"")
    sc_yr = input("Enter scenario year/tag: ")
    out_gdb = input("Enter path to output file geodatabase: ").strip("\"")

    # primary inputs - hard-coded for testing
    # model_run_dir = r'\\win11-model-1\D\SACSIM23\2050\DPS\2050_109_WAH3.5_FullTele23.5\run_folder'
    # sc_yr = 2050
    # out_gdb = r'I:\Projects\Darren\HiFrequencyTransit\HiFrequencyTransit.gdb'

    # CSV of stops that qualify as major but are not included in SACSIM (e.g. Colfax Amtrak station, possibly TRPA BRT stops)
    stops_to_add = None # Path(__file__).parent.joinpath('extra_major_transtops.csv')

    keep_all_nodes = True # do you want final outputs to keep all transit nodes? Or just those with qualifying HQT?
    buffer_dist_ft = None # set to None if you do not want a buffer createdf

    #======================================
    hqts = HQTransitStops(model_run_dir, scen_yr=sc_yr, keep_all=keep_all_nodes, addstops_csv=stops_to_add)
    final_stopsdf = hqts.make_hq_stop_df()
    point_fc_path = hqts.export_to_esri_fc(final_stopsdf, output_gdb=out_gdb)

    if buffer_dist_ft:
        arcpy.AddMessage(f"Creating {buffer_dist_ft}ft buffer...")
        pth_pts = Path(point_fc_path)
        buff_fc_name = f"{pth_pts.name}_hmbuff"
        buff_path = str(pth_pts.parent.joinpath(buff_fc_name))
        arcpy.analysis.Buffer(in_features=point_fc_path, out_feature_class=buff_path, 
                              buffer_distance_or_field=buffer_dist_ft,
                              dissolve_option='ALL')
    
    print("Script completed!")

