"""
Name: major_stop_identifier.py
Purpose: Using language from CA Public Resource Code Section 21064.3, tag applicable SACSIM bus stops as being major bus stops, which include:
    -rail stops,
    -BRT stops, OR
    -stops serving 2 or more local bus routes (non-BRT) with headway <=15mins in both AM and PM peak periods.
        NOTE - in some cases, a slightly higher max (e.g. 17mins) is used to account for "shoulder effect",
        e.g., for 5am-9am period, some routes start 15min service at 5:30am, so although technically their 5am-9am headway is 
        >15mins, from a standpoint of the policy purpose of frequency, we consider them to be 15min headways.


Author: Darren Conly
Last Updated: Nov 2022
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
    def __init__(self, in_tranline_txt, keep_all, addstops_csv=None, hf_th_mins=15):
        # Parse transit line file, split into node table and line table, as dataframe
        self.keep_all = keep_all # if false, only keep nodes that are HQ transit nodes
        self.in_tranline_txt = in_tranline_txt
        self.tranline_data = LinesNodes(self.in_tranline_txt)
        self.addstops_csv = addstops_csv

        self.line_data = self.tranline_data.line_rows_dict


        self.node_data = self.tranline_data.node_rows
        self.fnames_node_data = self.tranline_data.node_attrs_out_order

        self.color_brt = 4
        self.mode_rail = 1
        self.dir_tags = ['_A', '_B']
        self.f_name_nodir = 'LNAME_NODIR'

        self.hf_th_mins = hf_th_mins # max headway, in minutes, at which a line can be considered hi frequency

        self.sr_sacog = 2226

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

        # routes that are coded as express buses, but that are actually rail and thus
        #their stops are considered hi-quality.
        self.rail_exceptions = ['AMTRCC_A', 'AMTRCC_B', 'AMTRCCS_A', 'AMTRCCS_B',
                                'ZF_AMTRCCR_A', 'ZF_AMTRCCR_B', 'ZF_AMTRJPA_A', 'ZF_AMTRJPA_B']

        df_lines = pd.DataFrame(self.line_data)[linecols]

        # Add field to line df that strips direction tag (_A, _B) from line name
        df_lines[self.f_name_nodir] = df_lines[self.tranline_data.f_linename] \
                                        .apply(lambda x: self.strip_dirtag(x))

        # Filter line df to only include lines where: both headway1 and headway3 are >0 and <=15 OR
        #     COLOR is for BRT (4) OR MODE is for rail (1) OR
        #     NAME is one of the "rail exceptions", ie., rail routes with MODE/COLOR of express buses but which are actually rail
        # df_lines_hq = df_lines.loc[((df_lines[self.tranline_data.headway1] > 0) & (df_lines[self.tranline_data.headway1] <= self.hf_th_mins) \
        #                         & (df_lines[self.tranline_data.headway3] > 0) & (df_lines[self.tranline_data.headway3] <= self.hf_th_mins)) \
        #                         | (df_lines[self.tranline_data.color] == self.color_brt) \
        #                         | (df_lines[self.tranline_data.mode] == self.mode_rail) \
        #                         | (df_lines[self.tranline_data.f_linename].isin(self.rail_exceptions))
        #                     ]

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

        # Filter stops df to only include stop nodes
        self.df_nodes = df_nodes_raw.loc[df_nodes_raw[self.tranline_data.stopflag] == self.tranline_data.val_stop]

        # Add field to stop df that strips direction tag (_A, _B) from line name
        self.df_nodes[self.f_name_nodir] = self.df_nodes[self.tranline_data.f_linename] \
                                        .apply(lambda x: self.strip_dirtag(x))

        cols_nodir = [c for c in self.df_nodes.columns if c != self.tranline_data.f_linename]
        df_nodes_nodir = self.df_nodes[cols_nodir].drop_duplicates()

        return df_nodes_nodir

    def get_node_svc_data(self, df_lnd_data, node_id):
        # For each node ID:
        f_node = self.tranline_data.f_node_attrname
        f_mode = self.tranline_data.mode
        f_color = self.tranline_data.color
        f_name = self.f_name_nodir

        self.rail_exceptions_trimmed = [self.strip_dirtag(n) for n in self.rail_exceptions]

        
        #     df_node = filter dfjoin to only have rows with node ID
        df_node = df_lnd_data.loc[df_lnd_data[f_node] == node_id]

        #     df_node_rail = filter df_node to only have rows where MODE is for rail or where NAME is in list of "rail coded as express bus" routes, like Amtrak
        #         ***NOTE - will manually need to convert some Amtrak routes to rail after the fact because
        #             they're coded with mode as commuter bus
        df_node_rail = df_node.loc[(df_node[f_mode] == self.mode_rail) \
                            | (df_node[f_name].isin(self.rail_exceptions_trimmed))]

        #     df_node_brt = filter df_node to only have rows where color is BRT 
        df_node_brt = df_node.loc[(df_node[f_color] == self.color_brt)]
        import pdb; pdb.set_trace()

        #     df_node_hifreq = filter df_node to only have rows where COLOR is not BRT and MODE is not rail and it's not an AMTRAK exception
        # UPDATE 10/25/2024 - want to still flag hi-freq intersections where it's BRT, because of shifting definitions of BRT. But
        # want to be sure that these are counted as major due to frequency regardless of whether lines COLORed as BRT are BRT from legal standpoint.

        df_node_hifreq = df_node.loc[(df_node[f_mode] != self.mode_rail) \
                                    # & (df_node[f_color] != self.color_brt) \ #see note from 10/25/2024 above re: why BRT isn't qualifying criteria.
                                    & (~df_node[f_name].isin(self.rail_exceptions_trimmed) \
                                    & (df_node[self.tranline_data.headway3] > 0) & (df_node[self.tranline_data.headway3] <= self.hf_th_mins) \
                                    & (df_node[self.tranline_data.headway1] > 0) & (df_node[self.tranline_data.headway1] <= self.hf_th_mins) \
                                    ) \
                                ]
        
        

        all_lines = ';'.join(df_node[f_name]) if df_node.shape[0] > 0 else ''
        rail_lines = ';'.join(df_node_rail[f_name]) if df_node_rail.shape[0] > 0 else ''
        brt_lines = ';'.join(df_node_brt[f_name]) if df_node_brt.shape[0] > 0 else ''
        hi_freq_lines = ';'.join(df_node_hifreq[f_name]) if df_node_hifreq.shape[0] >= 1 else ''
        cnt_hifreqlines = df_node_hifreq.shape[0]

        # if node_id == '4427': import pdb; pdb.set_trace()


        out_data = {self.tranline_data.f_node_attrname: node_id, 
            self.k_all_lines: all_lines, self.k_rail_lines: rail_lines, self.k_brt_lines: brt_lines,
            self.k_hi_freq_lines: hi_freq_lines, self.k_cnt_hifreqlines: cnt_hifreqlines}

        return out_data

    def compare_lists(self, l1, l2):
        # compares stop nodes for 2 lines. If all of one line's stops are in
        # the other line's stop list, or vice-versa, warn that the lines serve
        # the same stops and that perhaps those stops shouldn't be eligible under SB743

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
        self.f_overlap = "overlap_warning"

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
                        overlap_warning = f"{overlap_warning}{lname}-{l_comp};"
                        data = [node, overlap_warning]
                        node_overlap_data.append(data)

        df_node_overlap = pd.DataFrame(node_overlap_data, columns=[self.tranline_data.f_node_attrname, self.f_overlap])

        result_df = in_hqt_node_df.merge(df_node_overlap, on=self.tranline_data.f_node_attrname, how='left')
        result_df[self.f_overlap].fillna('')

        return result_df
        

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
        self.k_majstoptyp = 'maj_stop'

        # Join consolidated lines df to stop nodes df (inner join) on stripped line name.
        self.df_linenode_joined = df_stopnodes.merge(df_hq_lines, on=self.f_name_nodir)

        node_svc_data = []
        for node in stopnode_list:
            node_data = self.get_node_svc_data(self.df_linenode_joined, node)
            node_svc_data.append(node_data)

        self.df_hqt_nodes = pd.DataFrame(node_svc_data)
        self.final_df = self.flag_dup_svc(self.df_hqt_nodes)
        self.final_df[self.tranline_data.f_node_attrname] = self.final_df[self.tranline_data.f_node_attrname].astype('int')

        # consolidate to single field that identifies what type of major stop it is
        self.final_df[self.k_majstoptyp] = 'Non-major stop' # default value: not a major transit stop
        self.final_df.loc[~self.final_df[self.k_rail_lines].isin(["", " "]), self.k_majstoptyp] = 'Rail'
        self.final_df.loc[~self.final_df[self.k_brt_lines].isin(["", " "]), self.k_majstoptyp] = 'BRT'
        self.final_df.loc[self.final_df[self.k_cnt_hifreqlines] > 1, self.k_majstoptyp] = '2+ Hi-frequency bus routes' # NOTE - will overwrite BRT tags where there's also hi-frequency intersection

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

    def export_to_esri_fc(self, node_dbf, output_gdb):
        """Join table of high-qual transit stops to X/Y data for each node from
        Cube hwy net, then export to SHP"""
        from arcgis.features import GeoAccessor, GeoSeriesAccessor
        
        f_x = 'X'
        f_y = 'Y'

        hwyfields = [self.tranline_data.f_node_attrname, f_x, f_y]
        df_hwynodes = esri_object_to_df(in_esri_obj=node_dbf, esri_obj_fields=hwyfields)
        
        sedf_join = df_hwynodes.merge(self.final_df, on=self.tranline_data.f_node_attrname)
        sedf_out = pd.DataFrame.spatial.from_xy(sedf_join, x_column=f_x, y_column=f_y, sr=self.sr_sacog)
        sedf_out[self.f_overlap] = sedf_out[self.f_overlap].fillna('')

        if self.addstops_csv: 
            sedf_out = self.manual_stop_add(sedf_out, self.addstops_csv)

        self.scen = Path(self.in_tranline_txt).stem
        self.tsufx = str(dt.datetime.now().strftime('%Y%m%d_%H%M'))
        out_fc = f"HQTStops_{self.scen}{self.tsufx}"
        out_path = str(Path(output_gdb).joinpath(out_fc))
        sedf_out.spatial.to_featureclass(out_path)

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
    in_tranline_lin = r"\\win11-model-1\d$\SACSIM19\2040\SACSIM19.04.01_2040_baseline\SACSIM19.04.01_2040_baseline\pa40_tranline.lin"  # r"Q:\SACSIM23\Network\SM23GIS\SHP\UD4H Data\2020_tranline_20221216.lin" 
    hwy_net = r"\\win11-model-1\d$\SACSIM19\2040\SACSIM19.04.01_2040_baseline\SACSIM19.04.01_2040_baseline\pa40_base.net"
    out_gdb = r"I:\Projects\Darren\HiFrequencyTransit\HiFrequencyTransit.gdb" # r'Q:\SACSIM23\Network\SM23GIS\MajorTransitStops.gdb'
    stops_to_add = Path(__file__).parent.joinpath('extra_major_transtops.csv')


    hf_threshold_mins = 20.9 # max headway in mins to qualify as hi-freq--though in some cases may want to nudge up to account for "shouldering"
    keep_all_nodes = False # do you want final outputs to keep all transit nodes? Or just those with qualifying HQT?

    buffer_dist_ft = 2640 # set to None if you do not want a buffer created

    #======================================
    sc_yr = int(input('Enter scenario year: '))
    hwy_node_dbf = npc.net2dbf(hwy_net, scenario_prefix=sc_yr, skip_if_exists=True)

    hqts = HQTransitStops(in_tranline_lin, keep_all=keep_all_nodes, hf_th_mins=hf_threshold_mins, 
                          addstops_csv=stops_to_add)
    hqts.make_hq_stop_df()
    point_fc_path = hqts.export_to_esri_fc(node_dbf=hwy_node_dbf, output_gdb=out_gdb)

    if buffer_dist_ft:
        arcpy.AddMessage(f"Creating {buffer_dist_ft}ft buffer...")
        pth_pts = Path(point_fc_path)
        buff_fc_name = f"{pth_pts.name}_hmbuff"
        buff_path = str(pth_pts.parent.joinpath(buff_fc_name))
        arcpy.analysis.Buffer(in_features=point_fc_path, out_feature_class=buff_path, 
                              buffer_distance_or_field=buffer_dist_ft,
                              dissolve_option='ALL')
    
    print("Script completed!")

