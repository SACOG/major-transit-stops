"""
Name: major_stop_identifier.py
Purpose: Using language from CA Public Resource Code Section 21064.3, tag applicable SACSIM bus stops as being major bus stops, which include:
    -rail stops,
    -BRT stops (as of 12/16/2024, BRT = 15min or less headways AND served by bus-only lanes), OR
    -stops serving 2 or more local bus routes (non-BRT) with headway <=20mins in both AM and PM peak periods.
        NOTE - in some cases, a slightly higher max (e.g. 17mins) is used to account for "shoulder effect",
        e.g., for 5am-9am period, some routes start 15min service at 5:30am, so although technically their 5am-9am headway is 
        >15mins, from a standpoint of the policy purpose of frequency, we consider them to be 15min headways.


Author: Darren Conly
Last Updated: Jan 2025
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""
from pathlib import Path
import datetime as dt

import arcpy
arcpy.env.overwriteOutput = True

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

        self.color_brt = 4
        self.mode_rail = 1
        self.dir_tags = ['_A', '_B']
        self.f_name_nodir = 'LNAME_NODIR'
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

    def create_line_df(self):
        # for line table, only keep fields for line name, mode, color, headway1, headway3

        linecols = [self.tranline_data.f_linename, self.tranline_data.mode,
                    self.tranline_data.color, self.tranline_data.headway1, 
                    self.tranline_data.headway3]

        df_lines = pd.DataFrame(self.line_data)[linecols]

        # Add field to line df that strips direction tag (_A, _B) from line name
        df_lines[self.f_name_nodir] = df_lines[self.tranline_data.f_linename] \
                                        .apply(lambda x: self.strip_dirtag(x))

        # Create consolidated lines df that is only unique lines after having direction stripped
        df_lines_nodir_cols = [self.tranline_data.mode, self.tranline_data.color, 
                            self.tranline_data.headway1, self.tranline_data.headway3]

        # df_lines_out = df_lines_hq.groupby(self.f_name_nodir)[df_lines_nodir_cols].min().reset_index()
        df_lines_out = df_lines.groupby(self.f_name_nodir)[df_lines_nodir_cols].min().reset_index()

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
        # f_color = self.tranline_data.color
        f_name = self.f_name_nodir

        self.rail_exceptions_trimmed = [self.strip_dirtag(n) for n in self.rail_exceptions]

        
        # df_node = filter dfjoin to only have rows with node ID
        df_node = df_lnd_data.loc[df_lnd_data[f_node] == node_id]

        # df_node_rail = filter df_node to only have rows where MODE is for rail or where NAME is in list of "rail coded as express bus" routes, like Amtrak
        #         ***NOTE - will manually need to convert some Amtrak routes to rail after the fact because
        #             they're coded with mode as commuter bus
        df_node_rail = df_node.loc[(df_node[f_mode] == self.mode_rail) \
                            | (df_node[f_name].isin(self.rail_exceptions_trimmed))]

        # OLD df_node_brt = filter df_node to only have rows where color is BRT 
        # df_node_brt = df_node.loc[(df_node[f_color] == self.color_brt)]

        # NEW - find rows that count as BRT major stops where both served by bus-only lane and where headway is meets frequency threshold
        df_node_brt = df_node.loc[(df_node[f_mode] != self.mode_rail) # non-rail 
                                    & (~df_node[f_name].isin(self.rail_exceptions_trimmed)) # non-rail-exception
                                    & (df_node[self.tranline_data.headway3] > 0) & (df_node[self.tranline_data.headway3] <= self.hifreq_brt_th) # hi-freq in AM
                                    & (df_node[self.tranline_data.headway1] > 0) & (df_node[self.tranline_data.headway1] <= self.hifreq_brt_th)  # hi_freq in PM
                                    & (df_node[self.f_buslane] > 0) # served by buslanes
                                ]

        # df_node_hifreq = filter df_node to only have rows where COLOR is not BRT and MODE is not rail and it's not an AMTRAK exception
        # UPDATE 10/25/2024 - want to still flag hi-freq intersections where it's BRT, because of shifting definitions of BRT. But
        # want to be sure that these are counted as major due to frequency regardless of whether lines COLORed as BRT are BRT from legal standpoint.

        df_node_hifreq = df_node.loc[(df_node[f_mode] != self.mode_rail)
                                    & (~df_node[f_name].isin(self.rail_exceptions_trimmed))
                                    & (df_node[self.tranline_data.headway3] > 0) & (df_node[self.tranline_data.headway3] <= self.hifreq_ix_th)
                                    & (df_node[self.tranline_data.headway1] > 0) & (df_node[self.tranline_data.headway1] <= self.hifreq_ix_th)
                                ]

        all_lines = ';'.join(df_node[f_name]) if df_node.shape[0] > 0 else ''
        rail_lines = ';'.join(df_node_rail[f_name]) if df_node_rail.shape[0] > 0 else ''
        brt_lines = ';'.join(df_node_brt[f_name]) if df_node_brt.shape[0] > 0 else ''
        hi_freq_lines = ';'.join(df_node_hifreq[f_name]) if df_node_hifreq.shape[0] >= 1 else ''
        cnt_hifreqlines = df_node_hifreq.shape[0]


        out_data = {self.tranline_data.f_node_attrname: node_id, 
            self.k_all_lines: all_lines, self.k_rail_lines: rail_lines, self.k_brt_lines: brt_lines,
            self.k_hi_freq_lines: hi_freq_lines, self.k_cnt_hifreqlines: cnt_hifreqlines}

        return out_data

    def compare_lists(self, l1, l2):
        # compares stop nodes for 2 lines. If all of one line's stops are in
        # the other line's stop list, or vice-versa, warn that the lines serve
        # the same stops and that perhaps those stops shouldn't be eligible under SB743

        # NOTE - could have all L1 stops within L2 route, but not vice-versa (e.g. if L1 is short version of L2)
        # even in this case and with both routes having <20min headways, do not consider separate lines because 
        # they do not provide hi-freq service to different places, e.g., you have hi-freq service for L2's stops,
        # and L1's hi frequency does not give you any additional coverage.

        l1_in_l2 = all([i in l2 for i in l1]) # True if all nodes in l1 are in l2
        l2_in_l1 = all([i in l1 for i in l2]) # True if all in l2 are in l1

        if l1_in_l2 or l2_in_l1:
            result = True
        else:
            result = False
        
        return result
        

    def flag_dup_svc(self, in_hqt_node_df):
        """
        # if a node has 2+ hi-freq routes, confirm that there's not perfect overlap. Examples of overlap:
        route A serves 1-2-3-4 and route B serves 2-3. Nodes 2 and 3 should *not* count as high-quality
        transit stop because routes A and B do not have substantively different service areas.
        """

        # get list of nodes where > hi frequency route, no brt, and no rail
        df_hf = in_hqt_node_df.loc[(in_hqt_node_df[self.k_rail_lines] == '') \
                                    & (in_hqt_node_df[self.k_brt_lines] == '') \
                                    & (in_hqt_node_df[self.k_cnt_hifreqlines] > 1)]

        hf_nodes = df_hf[self.tranline_data.f_node_attrname].unique()
        self.f_overlap = "overlap_lines"

        node_overlap_data = []
        for node in hf_nodes:
            # split out the names of the hi-freq routes
            try:
                lnames = df_hf.loc[df_hf[self.tranline_data.f_node_attrname] == node][self.k_hi_freq_lines] \
                    .values[0].split(';')
            except:
                import pdb; pdb.set_trace()
                
            tdict = {}
            for lname in lnames:
                lnodes = list(self.df_nodes.loc[self.df_nodes[self.f_name_nodir] == lname] \
                    [self.tranline_data.f_node_attrname])
                tdict[lname] = lnodes
                
            overlap_warning = ''
            for lname in tdict.keys():
                comparison_lines = [l for l in lnames if l != lname]
                for l_comp in comparison_lines:
                    has_overlap = self.compare_lists(tdict[lname], tdict[l_comp])
                    if has_overlap:
                        olap = f"{lname}-{l_comp}"
                        olap_rev = f"{l_comp}-{lname}"
                        if olap_rev in overlap_warning:
                            # e.g., if line pair A-B already flagged as overlapping pair, we do not want to also flag B-A.
                            pass
                        else:
                            overlap_warning = f"{overlap_warning}{olap};"
                            data = [node, overlap_warning]
                            node_overlap_data.append(data)

        df_node_overlap = pd.DataFrame(node_overlap_data, columns=[self.tranline_data.f_node_attrname, self.f_overlap])

        result_df = in_hqt_node_df.merge(df_node_overlap, on=self.tranline_data.f_node_attrname, how='left')
        result_df[self.f_overlap].fillna('', inplace=True)

        return result_df
    
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
        self.k_rail_lines = "rail_lines"
        self.k_brt_lines = "brt_lines"
        self.k_hi_freq_lines = "hifreq_lines"
        self.k_cnt_hifreqlines = "cnt_hifreqlines"
        self.k_net_hifreqlines = "net_hifreqlines"
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

        node_svc_data = []
        for node in stopnode_list:
            node_data = self.get_node_svc_data(self.df_linenode_joined, node)
            node_svc_data.append(node_data)

        self.df_hqt_nodes = pd.DataFrame(node_svc_data)
        self.final_df = self.flag_dup_svc(self.df_hqt_nodes)
        self.final_df[self.tranline_data.f_node_attrname] = self.final_df[self.tranline_data.f_node_attrname].astype('int')

        # consolidate to single field that identifies what type of major stop it is
        self.final_df[self.k_majstoptyp] = self.not_maj # default value: not a major transit stop
        self.final_df.loc[~self.final_df[self.k_rail_lines].isin(["", " "]), self.k_majstoptyp] = self.maj_rail
        self.final_df.loc[~self.final_df[self.k_brt_lines].isin(["", " "]), self.k_majstoptyp] = self.maj_brt
        
        def get_hifreq_ix(row):
            # tag as intersection of hi-freq lines if served by 2+ lines and those lines do not overlap too much.
            # notably, this function will not overwrite other major stop types. E.g., if stop already tagged as BRT, it will not overwrite the BRT tag.

            if (row[self.k_cnt_hifreqlines] > 1) & (row[self.k_majstoptyp] == self.not_maj):
                if (row[self.f_overlap] != ''):
                    # only apply this to stops that have >1 hifreq line and non-null risk of overlap
                    osplit = [i for i in row[self.f_overlap].split(';') if len(i) > 0] # list of potential overlap routes
                    hfl_list = row[self.k_hi_freq_lines].split(';') # list of hi-freq routes, including those that are overlapping

                    for lpair in osplit:
                        l1 = lpair.split('-')[0]
                        l2 = lpair.split('-')[1]
                        hfl_list = [i for i in hfl_list if i not in [l1, l2]] 
                        hfl_list.append(lpair)

                    if len(hfl_list) > 1:
                        row[self.k_majstoptyp] = self.maj_ix
                    
                    row[self.k_net_hifreqlines] = len(hfl_list)
                else:
                    # if >1 hifreq line but no overlap risk, set as major intersection
                    row[self.k_majstoptyp] = self.maj_ix
            else:
                pass

            return row

        # get more accurate determination of 2+ lines after removing potential overlapping lines that are more effecively just one line
        # e.g. routes P/Q in Davis, which are 2 hi-freq routes but are just opposite directions of same loop, so effectively one route
        self.final_df[self.k_net_hifreqlines] = self.final_df[self.k_cnt_hifreqlines]
        self.final_df = self.final_df.apply(get_hifreq_ix, axis=1)

        if not self.keep_all:
            # option to exclude non-major stops from output
            self.final_df = self.final_df.loc[(self.final_df[self.k_majstoptyp] != 'Non-major stop')]
        
        return self.final_df
    
    def manual_stop_add(self, sedf_in, stops_csv, csv_crs_id=4326):
        # manually add stops that are not in SACSIM stop network (e.g., Colfax train station)
        dfstops = pd.read_csv(stops_csv)
        sdf_stops = pd.DataFrame.spatial.from_xy(dfstops, x_column='x', y_column='y', sr=csv_crs_id)
        sdf_stops.spatial.project(self.sr_sacog)
        sdf_stops = sdf_stops[[f for f in sdf_stops.columns if f in sedf_in.columns]]

        df_out = pd.concat([sedf_in, sdf_stops])

        return df_out

    def export_to_esri_fc(self, output_gdb):
        """Join table of high-qual transit stops to X/Y data for each node from
        Cube hwy net, then export to SHP"""
        from arcgis.features import GeoAccessor, GeoSeriesAccessor

        node_x = 'X'
        node_y = 'Y'
        hwy_node_fields = [self.f_nodeid, node_x, node_y]
        hwy_node_dbf = npc.net2dbf(self.hwy_net, scenario_prefix=self.scen_yr, skip_if_exists=True)
        df_hwynodes = esri_object_to_df(in_esri_obj=hwy_node_dbf, esri_obj_fields=hwy_node_fields)
        
        sedf_join = df_hwynodes.merge(self.final_df, on=self.tranline_data.f_node_attrname)
        sedf_out = pd.DataFrame.spatial.from_xy(sedf_join, x_column=node_x, y_column=node_y, sr=self.sr_sacog)
        sedf_out[self.f_overlap] = sedf_out[self.f_overlap].fillna('')

        if self.addstops_csv: 
            sedf_out = self.manual_stop_add(sedf_out, self.addstops_csv)

        self.scen = Path(self.in_tranline_txt).stem
        self.tsufx = str(dt.datetime.now().strftime('%Y%m%d_%H%M'))
        out_fc = f"HQTStops_{self.scen}{self.tsufx}"
        out_path = str(Path(output_gdb).joinpath(out_fc))
        sedf_out.spatial.to_featureclass(out_path, sanitize_columns=False)

        user_msg = f"""Success! Output file is {out_path}
        
        WARNING: FINAL INSPECTIONS NEEDED:
            1. Nodes that have a value for the {self.f_overlap} field do not qualify as high-quality
                if they are served only by two non-rail, non-BRT, hi-frequency bus lines whose routes completely overlap.
            2. Make a general scan and make sure that nodes are reasonably close to real streets. Remember that high-quality
                stop locations dictate state-enforced land use regulations. 
            3. There are may be exceptions to this script's rules. Any maps should be vetted with local jursidictions
                and transit operators before external release.
            """
        arcpy.AddMessage(user_msg)

        return out_path

    def export_to_csv(self, output_dir):
        out_csv = f"HQTStops_{self.scen}{self.tsufx}"        
        self.final_df.to_csv(Path(output_dir).joinpath(out_csv), index=False)


if __name__ == '__main__':
    # primary inputs
    #model_run_dir = input("Enter model run folder path: ").strip("\"")
    #sc_yr = input("Enter scenario year/tag: ")
    #out_gdb = input("Enter path to output file geodatabase: ").strip("\"")

    # primary inputs - hard-coded for testing
    model_run_dir = r'\\win11-model-2\D\SACSIM23\2035\DPS\2035_180_WAH3.5_FullTele23.5\run_folder_comments_09122025_telewrk'#'\\win11-model-2\D\SACSIM23\2020\2020_67_Superwalk3\run_folder'#'\\win11-model-1\D\SACSIM23\2050\DPS\2050_109_WAH3.5_FullTele23.5\run_folder_comments_09122025_telew'
    sc_yr = 2035
    out_gdb = r'Q:\SACSIM23\Transit\HFTA_layers_finalized\HFTA_layers_finalized.gdb'

    # CSV of stops that qualify as major but are not included in SACSIM (e.g. Colfax Amtrak station, possibly TRPA BRT stops)
    stops_to_add = Path(__file__).parent.joinpath('extra_major_transtops.csv')

    keep_all_nodes = False # do you want final outputs to keep all transit nodes? Or just those with qualifying HQT?
    buffer_dist_ft = 2640 # set to None if you do not want a buffer createdf

    #======================================
    hqts = HQTransitStops(model_run_dir, scen_yr=sc_yr, keep_all=keep_all_nodes, addstops_csv=stops_to_add)
    hqts.make_hq_stop_df()
    point_fc_path = hqts.export_to_esri_fc(output_gdb=out_gdb)

    if buffer_dist_ft:
        arcpy.AddMessage(f"Creating {buffer_dist_ft}ft buffer...")
        pth_pts = Path(point_fc_path)
        buff_fc_name = f"{pth_pts.name}_hmbuff"
        buff_path = str(pth_pts.parent.joinpath(buff_fc_name))
        arcpy.analysis.Buffer(in_features=point_fc_path, out_feature_class=buff_path, 
                              buffer_distance_or_field=buffer_dist_ft,
                              dissolve_option='NONE')
    
    print("Script completed!")

