"""
Name: netpyconvert.py
Purpose: Functions you can call in a python script that convert a Cube NET file
    to common GIS or table files like dbf, link shp, etc.


Author: Darren Conly
Last Updated: 
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""
from pathlib import Path
import subprocess


def run_voyager_from_template(net_fpath, in_template_script, scenario_pref, script_params):
    formatted_script = net_fpath.with_stem(f"net2dbf").with_suffix(f'.s') # make copy of script with tokens filled, in model run folder

    with open(in_template_script, 'r') as f:
        str_script = f.read()

    # replace token values with correct values for input/output file
    str_script_formatted = str_script.format(**script_params)

    # export to script in model run folder that has correct token values filled in
    with open(formatted_script, 'w') as f2:
        f2.write(str_script_formatted)

    # run the filled-in script, outputting resulting file into model run folder
    # NOTE, for some reason it won't create the DBF if run in silent mode (adding arg "/s")
    subprocess.run(['Voyager.exe', str(formatted_script), f'-P{scenario_pref}', '/Start', '/Hide'], shell=True)

# convert NET to link or node DBF
def net2dbf(in_net_path, scenario_prefix, geom_type='NODE', out_dbf=None, skip_if_exists=False):
    # takes template voyager script, fills in file path args, then outputs DBF from a Cube NET file (in_net_path)
    net_file_path = Path(in_net_path)
    voyager_script_template = Path(__file__).parent.joinpath('net2dbf_template.s')
    
    if out_dbf is None:
        out_dbf = net_file_path.with_stem(f"{net_file_path.stem}{geom_type}").with_suffix(f'.dbf') # fpath of output DBF, if not provided by user
    
    out_dbf = Path(out_dbf)

    if skip_if_exists and out_dbf.exists():
        print(f"DBF {str(out_dbf)} already exists, so skipping its creation")
        pass

    else:
        print(f"Converting NET file {net_file_path.name} to {geom_type} DBF {out_dbf.name}...")
        params = dict(input_net=in_net_path, output_geom=geom_type, output_path=str(out_dbf))

        run_voyager_from_template(net_fpath=net_file_path, in_template_script=voyager_script_template,
                                scenario_pref=scenario_prefix, script_params=params)

    return str(out_dbf)


# Convert NET to link SHP
def net2linkshp(in_net_path, scenario_prefix, out_link_path=None, skip_if_exists=False):
    # takes template voyager script, fills in file path args, then outputs DBF from a Cube NET file (in_net_path)
    net_file_path = Path(in_net_path)
    voyager_script_template = Path(__file__).parent.joinpath('net2linkshp_template.s')
    
    if out_link_path is None:
        outshp_stem = f"{net_file_path.stem}LINKS"
        out_link_dir = net_file_path.parent.joinpath(outshp_stem)
        out_link_dir.mkdir(parents=True, exist_ok=True)

        out_link_path = out_link_dir.joinpath(outshp_stem).with_suffix(f'.shp') # fpath of output DBF, if not provided by user
    
    out_link_path = Path(out_link_path)

    if skip_if_exists and out_link_path.exists():
        print(f"Link SHP {str(out_link_path)} already exists, so skipping its creation")
        pass

    else:
        print(f"Converting NET file {net_file_path.name} to link SHP {out_link_path.name}...")

        params = dict(input_net=str(net_file_path), output_path=str(out_link_path))
        run_voyager_from_template(net_fpath=net_file_path, in_template_script=voyager_script_template,
                        scenario_pref=scenario_prefix, script_params=params)

        # run_voyager_from_template(net_fpath=net_file_path, in_template_script=voyager_script_template,
        #                         out_file_path=out_link_path, scenario_pref=scenario_prefix)

    return str(out_link_path)


if __name__ == '__main__':

    net_file = r'D:\SACSIM23\faresystem_change\SACSIM23.01.03_2016_baseline_2\2016_base.net'
    sc_token = 2016

    # dbf_out = net2dbf(in_net_path=net_file, scenario_prefix=sc_token, out_dbf=None)
    # print(dbf_out)

    net2linkshp(in_net_path=net_file, scenario_prefix=sc_token, out_link_path=None)

