import arcpy
import pandas as pd
import os


def clean_transit_stops(fc_2050, fc_2020, remove_csv_path=None):
    """
    1. Creates a copy of the 2050 FC.
    2. Removes overlapping 2020 Rail stops.
    3. Removes manual CSV exclusions.
    4. Removes Nodes linked to updated BRT definition in Roseville (12026, 14647, 17205).
    """

    # ---------------------------------------------------------
    # Step 0: Create a Copy to Edit
    # ---------------------------------------------------------
    arcpy.env.overwriteOutput = True

    out_fc_name = os.path.basename(fc_2050) + "_updated"
    out_fc_path = os.path.join(os.path.dirname(fc_2050), out_fc_name)

    print(f"Creating copy of 2050 data: {out_fc_path}...")
    arcpy.management.CopyFeatures(fc_2050, out_fc_path)

    target_fc = out_fc_path
    f_node_id = "N"
    f_type = "maj_stop"
    val_rail = "Rail"

    # Define the specific nodes you requested to remove
    manual_node_ids = {12026, 14647, 17205}

    # ---------------------------------------------------------
    # Step 1: Build set of Rail Node IDs from the 2020 Baseline
    # ---------------------------------------------------------
    existing_rail_nodes = set()

    if arcpy.Exists(fc_2020):
        print(f"Reading existing rail stops from: {fc_2020}...")
        with arcpy.da.SearchCursor(fc_2020, [f_node_id, f_type]) as cur:
            for row in cur:
                node_val, stop_type = row
                if stop_type and stop_type.lower() == val_rail.lower():
                    existing_rail_nodes.add(node_val)
        print(f"Found {len(existing_rail_nodes)} existing rail nodes in 2020 layer.")
    else:
        arcpy.AddWarning(f"2020 Feature Class not found. Skipping overlap check.")

    # ---------------------------------------------------------
    # Step 2: Build set of Node IDs to remove from CSV
    # ---------------------------------------------------------
    csv_remove_nodes = set()
    if remove_csv_path and os.path.exists(remove_csv_path):
        print(f"Reading manual removal list from: {remove_csv_path}...")
        try:
            df_remove = pd.read_csv(remove_csv_path)
            if f_node_id in df_remove.columns:
                csv_remove_nodes = set(df_remove[f_node_id].unique())
                print(f"Found {len(csv_remove_nodes)} nodes to remove from CSV.")
            else:
                arcpy.AddWarning(f"Field '{f_node_id}' not found in CSV.")
        except Exception as e:
            arcpy.AddError(f"Error reading CSV: {e}")

    # ---------------------------------------------------------
    # Step 3: Remove features from the COPIED layer
    # ---------------------------------------------------------
    print(f"Starting removal process on: {os.path.basename(target_fc)}...")

    delete_count_rail = 0
    delete_count_csv = 0
    delete_count_manual = 0

    with arcpy.da.UpdateCursor(target_fc, [f_node_id, f_type]) as cur:
        for row in cur:
            node_val = row[0]
            stop_type = row[1]
            should_delete = False

            # Condition 1: Specific Manual Node ID list
            if node_val in manual_node_ids:
                should_delete = True
                delete_count_manual += 1

            # Condition 2: CSV Match
            elif node_val in csv_remove_nodes:
                should_delete = True
                delete_count_csv += 1

            # Condition 3: Rail Overlap
            elif stop_type and stop_type.lower() == val_rail.lower():
                if node_val in existing_rail_nodes:
                    should_delete = True
                    delete_count_rail += 1

            if should_delete:
                cur.deleteRow()

    print("---------------------------------------------------------")
    print(f"Success! Output saved to: {out_fc_path}")
    print(f"Removed {delete_count_manual} specific manual nodes ({', '.join(map(str, manual_node_ids))}).")
    print(f"Removed {delete_count_rail} stops due to 2020 Rail overlap.")
    print(f"Removed {delete_count_csv} stops due to CSV exclusion.")
    print("---------------------------------------------------------")

    return out_fc_path


if __name__ == '__main__':
    gdb_path = r'Q:\SACSIM23\Transit\HFTA_layers_finalized\HFTA_layers_finalized.gdb'
    fc_2020_path = os.path.join(gdb_path, "Major_Transit_Stops_PT2020")
    fc_2050_path = os.path.join(gdb_path, "Major_Transit_Stops_PT2050")
    csv_path = r'Q:\SACSIM23\Transit\HFTA_layers_finalized\stops_to_remove.csv'

    clean_transit_stops(fc_2050_path, fc_2020_path, csv_path)