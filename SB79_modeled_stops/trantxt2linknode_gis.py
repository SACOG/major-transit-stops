"""
Name: trantxt2linknode_gis.py
Purpose: convert tranline.txt file to separate node and route files.
    Choose "GDB" if you want to output to GIS line file and database tables of lines and notes.
    Choose "text" if you want to output to separate line and node text files.
    "Line" file is list of all lines with line-level attributes
    "Node" file is list of all transit nodes.


Author: Yanmei Ou/Darren Conly
Last Updated: Jun 2022
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

import os
import re
import datetime as dt
import arcpy

arcpy.env.overwriteOutput = True


#=============================FUNCTIONS==========================================
class LinesNodes:
    def __init__(self, in_txt):
        self.in_txt = in_txt

        
        self.f_node_attrname = 'N' #field name for node id
        self.node_long = 'NODE'
        self.nodes_long = 'NODES' # in LIN file, sometimes is written as NODES (with S) instead of NODE
        self.node_tf_name = 'TF'
        self.stopflag = 'STOP'
        self.seqno = 'SEQ'

        self.val_stop = 'Y' # value for stop node
        self.val_notstop = 'N' # not a stop node

        self.f_linename = 'LINE NAME'
        self.linename_gis = 'LINE_NAME'
        self.mode = 'MODE'
        self.color = 'COLOR'
        self.oneway = 'ONEWAY'
        self.faresystem = 'FARESYSTEM'
        self.operator = 'OPERATOR'
        self.circular = 'CIRCULAR'

        self.tf1 = self.get_period_1_name('TIMEFAC[1]')
        self.tf2 = 'TIMEFAC[2]'
        self.tf3 = 'TIMEFAC[3]'
        self.tf4 = 'TIMEFAC[4]'
        self.tf5 = 'TIMEFAC[5]'

        self.headway1 = self.get_period_1_name('HEADWAY[1]') # files exported from Cube GIS (11/9/2022) don't have HEADWAY[1], they just call it HEADWAY, if they even have service then
        self.headway2 = 'HEADWAY[2]'
        self.headway3 = 'HEADWAY[3]'
        self.headway4 = 'HEADWAY[4]'
        self.headway5 = 'HEADWAY[5]'


        self.line_attrs = [self.f_linename, self.tf1, self.tf2, self.tf3, self.tf4, self.tf5,
                         self.oneway, self.mode, self.faresystem, self.operator,
                         self.color, self.circular, self.headway1, self.headway2,
                         self.headway3, self.headway4, self.headway5]

        self.line_attrs_outorder = [self.f_linename, self.oneway, self.mode, self.faresystem, 
                                self.operator, self.color, self.circular, self.tf1, self.tf2, self.tf3, 
                                self.tf4, self.tf5, self.headway1, self.headway2, self.headway3, 
                                self.headway4, self.headway5]

        
        self.node_attrs_out_order = [self.f_linename, self.f_node_attrname, self.seqno,
                                     self.stopflag, self.node_tf_name]
        
        self.f_tf_attrnames = [self.node_tf_name, 'TIMEFAC'] # time factor field names
        

                
        self.data_rows = self.make_link_node_outputs(in_txt)
        self.line_rows_dict = self.data_rows[0] # each row contains line-level data {line attr name: attr value}
        self.line_rows_vals = [list(d.values()) for d in self.line_rows_dict] # each row is just list of attr values for each line
        self.node_rows = self.data_rows[1] # each row contains data for each node on each line

    def get_period_1_name(self, input_name):
        # Depending on where its exported from, there may or may not be [1] with period-specific attribute names.
        # E.g., HEADWAY[1] will simply be HEADWAY when exported from GIS
        output_name = input_name
        with open(self.in_txt, 'r') as f:
            f_str = f.read()

        if input_name not in f_str:
            output_name = input_name.replace('[1]','')

        if output_name not in f_str:
            raise Exception(f"{output_name} is not an attribute in {self.in_txt}")

        return output_name

    def get_line_attrs(self):  
        """
        Generates dictionary of line-level attributes, e.g.:
            {<line name>: <list of line attributes>}

        """
        line_dict_out = {}
        
        with open(self.in_txt, 'r') as f_in:
            lines = f_in.readlines()
            for line in lines:
                if len(line) != 0: 
                    line = line.strip() # removes any leading or trailing spaces
                    if line[0] != ';': #if line is not a cube commented-out line
                        if re.match(self.f_linename, line): #if it's the start of a new transit line feature
                            #line_attrs = ''
                            line_list = line.split(',') #make into comma-delimited list
                            line_name1 = line_list[0].split('=') # 'LINE NAME="AMTRCCB_A"' becomes list ['LINE NAME', '"AMTRCCB_A"']
                            line_name = line_name1[1].strip('"') #get the line name
                            line_attrs = line
                        elif line[-1] == ',': #if the line ends with a comma, it's part of the same route entry
                            line_attrs = line_attrs + line
                        else:
                            line_attrs = line_attrs + line
                            line_dict_out[line_name] = (line_attrs) #dict entry - {NAME:[NAME, TFs, HEADWAYs, node list, etc.]}
                            
        return line_dict_out
    
    def make_node_lists(self, line_attrs_str):
        """
        Creates dict with {node id: time factor at that node}.
        Example: if you have defaul TF of 1.2, then nodes 1 > 2 > TF=2.22 > 3...,
        Then the resulting dict would be {1: 1.2, 2: 1.2, 3: 2.22, ...}
        """
        
        # example of line_attrs_str: ['LINE NAME=LINE1','COLOR=2'...]
        line_attrs_list = line_attrs_str.split(',')

        node_tf_arr = [] # [[node, tf]...] # cannot use dict because sometimes same node will appear more than once and have different tf value
        
        tf_change = '0' # default value for time factor
        for attr in line_attrs_list: 
            attr_sp = attr.strip().split('=') # example: 'LINE NAME=LINE1' becomes ['LINE NAME', 'LINE1']
            
            if len(attr_sp) > 1: # if the attribute has a name to it as opposed to just being the value (most nodes don't have attrib names)
                attr_name = attr_sp[0] # attribute name (e.g. "N", "TF")
                attr_value = attr_sp[1].strip('"') # value of attribute
        
                # for each line, the node values will be the keys of the node_df_dict
                # if attr_name == self.f_node_attrname: 
                if attr_name in [self.f_node_attrname, self.nodes_long]: 
                    node_tf_arr.append([attr_value, tf_change])
                    
                # if there's a time factor change along the route, then appropriately set that nodes TF value
                elif attr_name in self.f_tf_attrnames: 
                    tf_change = attr_value
            else: # if the attribute doesn't have a name, then it's a node id
                node_val = attr_sp[0]
                node_tf_arr.append([node_val, tf_change])   
                
        return node_tf_arr
    
    def get_line_attr_dict(self, line_attrs_str):
        
        line_attrs_dict = {}
        
        for attr in line_attrs_str.split(','): 
            attr_sp = attr.strip().split('=') # example: 'LINE NAME=LINE1' becomes ['LINE NAME', 'LINE1']
            
            if len(attr_sp) > 1: # if the attribute has a name to it as opposed to just being the value (most nodes don't have attrib names)
                attr_name = attr_sp[0] # attribute name
                attr_value = attr_sp[1].strip('"') # value of attribute
                
                # non-node line attributes (e.g. line name, color, headways...)
                if attr_name in self.line_attrs:
                    line_attrs_dict[attr_name] = attr_value
            
        
        return line_attrs_dict
        
    
    def ideal_type(self, in_str):
        '''
        Takes string as input and, if possible, converts either to integer or float data type.
        '''
        
        try:
            re_az = re.compile('.*[a-zA-Z]+.*')
            re_decimal = re.compile('.*\..*')
            
            if re.match(re_az, in_str): # if has letters, is string
                out = in_str
            elif re.match(re_decimal, in_str): # if no letter but periods, is float
                out = float(in_str)
            else: # if no letter and no periods, then is integer
                out = int(in_str)
                
        except ValueError:
            out = in_str # if all else fails, output will be same as input (string)
            
        return out

    def make_link_node_outputs(self, in_file):
        try:
            print("Writing out line and node lists...")
            
            # {NAME:[NAME, TFs, HEADWAYs, node list, etc.]}
            lines_dict = self.get_line_attrs()

            line_dicts = [] # list of dicts [{attr:val for line 1}, {attr:val for line 2}...]
            node_rows = []

            for line_name in lines_dict.keys():
                line_attrs = lines_dict[line_name]
                line_attrs_dict = self.get_line_attr_dict(line_attrs)
                
                node_tf_array = self.make_node_lists(line_attrs)

                # put values into correct order to insert into output gdb; ensure all fields needed for GDB included
                # if an attribute name is not found int he file, then make it's value = '0'
                row_dict2 = {attrname: line_attrs_dict[attrname] if line_attrs_dict.get(attrname) else '0' \
                             for attrname in self.line_attrs_outorder} 

                # row_dict2 = {attrname: line_attrs_dict[attrname] if line_attrs_dict.get(attrname) else '0' \
                #              for attrname in self.line_attrs_outorder} 

                dkeys = list(row_dict2.keys())

                # make values into field data types compatible with the feature class created.
                for k in dkeys:
                    v_in = row_dict2[k]
                    if k == self.f_linename:
                        row_dict2[k] = v_in #but line name is always text, even if it's a number value
                    else: 
                        ideal_v = self.ideal_type(v_in)
                        row_dict2[k] = ideal_v
                
                line_dicts.append(row_dict2) # output link_row has line-level route info
                
                # generate node-level table
                for node_seq, ntf_pair in enumerate(node_tf_array):
                    node_signed = ntf_pair[0]
                    if node_signed[0] == '-': #if node has negative value, it's not a stop
                        stop = self.val_notstop
                        node = node_signed.strip('-') #take minus symbol out of node id
                    else:
                        stop = self.val_stop
                        node = node_signed
            		
                    tf = ntf_pair[1]
                    node_row = [line_name, node, node_seq, stop, tf]
                    node_rows.append(node_row)
            return (line_dicts, node_rows)
        except KeyError:
            print("Key error. The line after {} may not have all of its line-level fields. Please check." \
                  .format(line_name))
                
class textOutput:
    def __init__(self, in_file):
        self.in_file = in_file
        self.data = LinesNodes(self.in_file)
        
        self.line_rows = self.data.line_rows_vals
        self.node_rows = self.data.node_rows
        
        self.out_linktxt_header = ','.join(self.data.line_attrs_outorder) + '\n'
        self.out_nodetxt_header = ','.join(self.data.node_attrs_out_order) + '\n'
        
        # converts ".../filename.txt" to "file"
        self.in_txt_fname = os.path.splitext(os.path.basename(self.in_file))[0]
        
        # if output folder not specified by user, default will be to make a new subfolder within
        # the folder containing the transit line file, and separated line and node files
        # will be put into the subfolder.
        self.default_output_dir = os.path.join(os.path.dirname(in_file), "transit_linenode")
    
 
    def make_txt(self, output_dir=None):
        
        if output_dir is None:
            output_dir = self.default_output_dir
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
                
        
        out_lines_txt = f"{self.in_txt_fname}_lines.txt"
        out_nodes_txt = f"{self.in_txt_fname}_nodes.txt"
        
        output_lines_fpath = os.path.join(output_dir, out_lines_txt)
        output_nodes_fpath = os.path.join(output_dir, out_nodes_txt)
        
        
        with open(output_lines_fpath, 'w') as f_out_link:
            f_out_link.write(self.out_linktxt_header)
            for row in self.line_rows:
                row = ','.join(str(i) for i in row) + '\n'
                f_out_link.write(row)
        
        with open(output_nodes_fpath, 'w') as f_out_node:
            f_out_node.write(self.out_nodetxt_header)
            for row in self.node_rows:
                row = ','.join(str(i) for i in row) + '\n'
                f_out_node.write(row)
                
        print(f"Success! Output files are in {output_dir}")        
        
class GISOutput:
    def __init__(self, in_txt, in_node_dbf, output_dir, str_scen_yr):
        
        # data from transit line txt file (node-level and line-level rows)
        self.line_node_data = LinesNodes(in_txt)
        self.line_rows_dict = self.line_node_data.line_rows_dict
        self.node_rows = self.line_node_data.node_rows
        
        self.hwynode_dbf = in_node_dbf
        
        # workspace and locations
        self.scratch_gdb = arcpy.env.scratchGDB
        self.output_dir = output_dir # for now must be a GDB; in future should allow using folder (for SHP/DBF export too)
        
        # output file names
        self.date_sufx = str(dt.datetime.now().strftime('%m%d%Y_%H%M'))
        self.str_scen_yr = str_scen_yr
        self.link_tbl = "PT_link{}_{}".format(self.str_scen_yr, self.date_sufx)
        self.node_tbl = "PT_node{}_{}".format(self.str_scen_yr, self.date_sufx)
        self.link_fc = "PT_linkFC{}_{}".format(self.str_scen_yr, self.date_sufx) # name of output feature class of transit link feature class
        
        
        # column naming
        self.colname_lookup = {} # ideally this should be synced up/connected to header names on input txt file
        
        self.spatial_ref = arcpy.SpatialReference(2226)

    def format_gis_fname(self, in_str):
        # reformats attribute names so they can safely be used as GIS feature class field names
        underscore_chars = ['[', ']', ' ']

        for c in underscore_chars:
            in_str = in_str.replace(c, '_')
        
        return in_str

    # create gdb table of link-level data
    def create_link_tbl(self):
        print("writing link table...")
        link_tbl_fpath = os.path.join(self.output_dir, self.link_tbl)
        
        arcpy.CreateTable_management(self.output_dir, self.link_tbl,"","")

        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.linename_gis, "TEXT", field_length=20)
        # arcpy.AddField_management(link_tbl_fpath,"TIMEFAC", "TEXT", field_length=5)
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.oneway, "TEXT", field_length=2)
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.mode, "SHORT")
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.faresystem, "SHORT")
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.operator, "SHORT")
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.color, "SHORT")
        arcpy.AddField_management(link_tbl_fpath, self.line_node_data.circular, "TEXT", field_length=2)

        for tf_field in [self.line_node_data.tf1, self.line_node_data.tf2,
                        self.line_node_data.tf3, self.line_node_data.tf4, self.line_node_data.tf5]:
            arcpy.AddField_management(link_tbl_fpath, tf_field, "FLOAT", field_length=5)

        for hdwy_field in [self.line_node_data.headway1, self.line_node_data.headway2,
                        self.line_node_data.headway3, self.line_node_data.headway4, 
                        self.line_node_data.headway5]:
            arcpy.AddField_management(link_tbl_fpath, hdwy_field, "SHORT")
        	
        link_fields = [i.name for i in arcpy.ListFields(link_tbl_fpath)]
        link_fields = link_fields[1:] #omit the OBJECTID field
        	
        link_inscur = arcpy.da.InsertCursor(link_tbl_fpath, link_fields)

        # ensure compatibility: if feature class has attributes not in LIN file, then put in null values
        # for those feature class fields. If LIN file has attributes not in FC, then do not add them to FC.
        for i, rowdict in enumerate(self.line_rows_dict):
            dict_fnames = {self.format_gis_fname(k):v  for k, v in rowdict.items()}
            out_row = [dict_fnames.get(fn) for fn in link_fields]

            link_inscur.insertRow(out_row)
        
        del link_inscur
    
    #create route node gdb table
    def create_node_tbl(self):
        print("writing node table...")
        
        temp_nodetbl = "TEMP_nodetbl"
        temp_nodetbl_fpath = os.path.join(self.scratch_gdb, temp_nodetbl) # temp table, prior to adding x/y values to nodes
        node_tbl_fpath = os.path.join(self.output_dir, self.node_tbl) # final output node table

        arcpy.CreateTable_management(self.scratch_gdb, temp_nodetbl)
        arcpy.AddField_management(temp_nodetbl_fpath, self.line_node_data.f_linename, "TEXT", field_length=20)
        arcpy.AddField_management(temp_nodetbl_fpath, self.line_node_data.node_long,"LONG")
        arcpy.AddField_management(temp_nodetbl_fpath, self.line_node_data.seqno,"LONG")
        arcpy.AddField_management(temp_nodetbl_fpath, self.line_node_data.stopflag,"TEXT", field_length=2)
        arcpy.AddField_management(temp_nodetbl_fpath, self.line_node_data.node_tf_name,"TEXT", field_length=5)
        
        node_fields = [i.name for i in arcpy.ListFields(temp_nodetbl_fpath)]
        node_fields = node_fields[1:] #omit the OBJECTID field
        	
        with arcpy.da.InsertCursor(temp_nodetbl_fpath, node_fields) as node_cursor:
            for row in self.node_rows:
                node_cursor.insertRow(row)
        
        #make fls of the all-network node table and the list of transit nodes  
        tv_allnetnodes = "tv_allnetnodes"
        tv_trannodes = "tv_trannodes"
        
        arcpy.MakeTableView_management(temp_nodetbl_fpath, tv_trannodes)
        arcpy.MakeTableView_management(self.hwynode_dbf, tv_allnetnodes)
        
        #add XY data to transit nodes via join with all-network nodes DBF
        net_fields = [self.line_node_data.f_node_attrname, "X", "Y"] # fields in network to include in join operation
        arcpy.JoinField_management(tv_trannodes, self.line_node_data.node_long, tv_allnetnodes, 
                                self.line_node_data.f_node_attrname, net_fields)
        
        # convert the table view, now with all node attribs and X/Y info, to a GDB table
        arcpy.TableToTable_conversion(tv_trannodes, self.output_dir, self.node_tbl)
        
        arcpy.DeleteField_management(node_tbl_fpath, self.line_node_data.f_node_attrname)
        arcpy.Delete_management(temp_nodetbl_fpath)
        
        
    #make SHP/FC of transit lines
    def make_line_fc(self):
        
        arcpy.env.qualifiedFieldNames = False
        arcpy.env.workspace = self.output_dir
        
        # create line and transit node tables
        self.create_link_tbl()
        self.create_node_tbl()
        
        print("making line FC...")
        
        temp_line_fc = os.path.join(self.scratch_gdb, "temp_line_fc")
        temp_trannode_fc = os.path.join(self.scratch_gdb, "tran_nodes_fl_copy")
        temp_nodejoin_tbl = "temp_nodejoin_tbl"
        temp_nodejoin_fpath = os.path.join(self.scratch_gdb, temp_nodejoin_tbl)
        
        #parameters to join model network fc nodes to table of nodes from tranline file
        fl_node_field = "N"
        tbl_node_field = "NODE"
        join_type = "KEEP_COMMON" #inner join
        
        #parameters for joining points into transit lines
        x_field = "X" #from full network node dbf's table 
        y_field = "Y"
        line_field = self.line_node_data.linename_gis
        sort_field = self.line_node_data.seqno
        
        #feature layer/table view names
        node_xy_tv = "node_xy_tv" #model net nodes to fl
        node_tblvw = "node_tblvw" #transit node list table to fl
        line_tblvw = "line_tblvw" #transit line table to fl
        line_fc_fl = "line_fc_fl" #output line fc to fl
        tran_nodes_fl = "tran_node_fl" #will be output of MakeXYEventLayer
        
        #make qualified field names ('table.field')
        arcpy.MakeTableView_management(self.hwynode_dbf, node_xy_tv) #make model node SHP into feature layer
        shp_name_prefix = arcpy.Describe(node_xy_tv).name
        shp_name_prefix = re.search('(.*)\..*',shp_name_prefix).group(1) #from 'xxx.shp' return 'xxx'

        arcpy.MakeTableView_management(self.node_tbl, node_tblvw)
        arcpy.MakeTableView_management(self.link_tbl, line_tblvw)
        
        #join transit node list to SHP of network nodes, keeping only nodes that are in transit file
        arcpy.AddJoin_management(node_tblvw, tbl_node_field, node_xy_tv, fl_node_field, join_type)

        #copy to temporary GDB in order to eliminate field prefixes
        arcpy.TableToTable_conversion(node_tblvw, self.scratch_gdb, temp_nodejoin_tbl)
        
        #make spatial FL of XY data
        arcpy.MakeXYEventLayer_management(temp_nodejoin_fpath, x_field, y_field, tran_nodes_fl, self.spatial_ref)
        
        #make FC of model transit lines from points
        arcpy.PointsToLine_management(tran_nodes_fl, temp_line_fc, line_field, sort_field)
        arcpy.MakeFeatureLayer_management(temp_line_fc, line_fc_fl)
        
        #join with link-level attributes, making sure column names are correct (not truncated join name columns)
        arcpy.AddJoin_management(line_fc_fl, line_field, line_tblvw, line_field, join_type)

        #output line feature layer to feature class, overwriting original fc
        arcpy.FeatureClassToFeatureClass_conversion(line_fc_fl, self.output_dir, self.link_fc) #arcpy.FeatureClassToFeatureClass_conversion(line_fc_fl,scratch_gdb,output_line_fc)
        
        output_link_fc_path = os.path.join(self.output_dir, self.link_fc)
        
        #delete unneeded columns
        for f in [f"{self.line_node_data.linename_gis}_1"]:
            if f in [f.name for f in arcpy.ListFields(output_link_fc_path)]:
                arcpy.DeleteField_management(output_link_fc_path,[f])

        arcpy.Delete_management(temp_line_fc)
        arcpy.Delete_management(temp_trannode_fc)
        
        print(f"Success! Created line feature class {os.path.join(self.output_dir, self.link_fc)}")
    
def do_work():
    tranline_txt = input('Enter file path for transit line txt file: ')
    output_format = input("Specify desired output format('GDB' or 'text'): ")
    # tranline_txt = r"Q:\SACSIM23\Network\TransitNetwork\Major Transit Stops\SACSIM23\2022_11\2020_tranline.lin"
    # output_format = 'gdb'
    
    if output_format.lower() == 'text':
        textOutput(tranline_txt).make_txt()
    elif output_format.lower() == 'gdb':
        hwy_node_dbf = input('Enter file path for hwy node *master network* DBF whose X/Y coordinates you will use: ')
        output_gdb = input("Enter the file path for the ESRI file geodatabase you want your outputs to be in: ")
        # hwy_node_dbf = r"Q:\SACSIM23\Network\TransitNetwork\Major Transit Stops\SACSIM23\2022_11\2020_base_nodes.dbf"
        # output_gdb = r"Q:\SACSIM23\Network\SM23GIS\SM23Testing.gdb"

        sc_yr = input("Enter the scenario year: ")
        # sc_yr = '2020'
        
        gis_obj = GISOutput(tranline_txt, hwy_node_dbf, output_gdb, sc_yr)
        gis_obj.make_line_fc()
    else:
        raise ValueError("Output format must be either 'GDB' or 'text'. Please try again using either 'GDB' or 'text' for the output format.")
        
            
            

#======================RUN SCRIPT============================================

if __name__ == '__main__':
    do_work()
    

