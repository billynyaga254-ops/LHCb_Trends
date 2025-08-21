#!/usr/bin/env python3
"""
plot_trends_publish_multi.py

Standalone script to publish VELO module efficiencies (per ASIC) for ALL sensors
to DQDB so MONET can overlay them. Also creates local verification PNGs.

Usage:
    python3 plot_trends_publish_multi.py <run_lower> <run_upper> <options_path>

Example:
    python3 plot_trends_publish_multi.py 310000 310999 tracking/publish_velo_all_sensors

Notes:
 - options_path should be the path to the options file (e.g., "tracking/publish_velo_all_sensors").
 - The script will now loop through all predefined VELO sensor names (VP00, VP01, etc.)
   and modules (0-51), publishing each individually.
"""

import sys
import os
import json
import time
import subprocess
from math import floor
from importlib import import_module as load_options
from pathlib import Path

# ROOT imports
import ROOT
from ROOT import TFile, TCanvas, TGraphErrors, TMultiGraph, TLegend

import numpy as np

# Local helper used to push data to DQDB (must be on PYTHONPATH)
from save_in_dqdb import save_in_dqdb

# ---------- Configuration ----------
ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

rundb_loc   = 'http://rundb-internal.lbdaq.cern.ch/api/run/'
rundb_info  = 'rundb_files'
rundb_time_format = '%Y-%m-%dT%H:%M:%S%z'
file_suffix = '.root'
min_run_length = 300.0    # seconds
require_offline = False
publish_to_dqdb = True

# *** New: Define all 12 VELO sensor names ***
VELO_SENSOR_NAMES = ['VP00', 'VP01', 'VP02', 'VP10', 'VP11', 'VP12',
                     'VP20', 'VP21', 'VP22', 'VP30', 'VP31', 'VP32']
TOTAL_MODULES = 52 # Modules 0-51

# ------------------------------------

def run_dir_from_run_no(run_no):
    sd_1 = floor(run_no/10000)*10000
    sd_2 = floor(run_no/1000)*1000
    return f"{sd_1}/{sd_2}/"

def usage_and_exit():
    print("Usage: python3 plot_trends_publish_multi.py <run_lower> <run_upper> <options_path>")
    print("Example: python3 plot_trends_publish_multi.py 310000 310999 tracking/publish_velo_all_sensors")
    sys.exit(1)

# ----------- Argument parsing -----------
if len(sys.argv) < 4:
    usage_and_exit()

try:
    run_lower = int(sys.argv[1])
    run_upper = int(sys.argv[2])
except ValueError:
    print("Run numbers must be integers.")
    usage_and_exit()

if run_upper < run_lower:
    print("Upper run number must be >= lower run number.")
    sys.exit(1)

options_path = sys.argv[3]  # e.g. tracking/publish_velo_all_sensors

# Load options module
try:
    options = load_options(f"options.{options_path.replace('/', '.') }").options()
except Exception as e:
    print(f"Error loading options file '{options_path}': {e}")
    sys.exit(1)

# Read git key
if not os.path.exists('.git_key_file'):
    print("Error: .git_key_file not found in working directory. This file must contain git key for DQDB.")
    sys.exit(1)
git_key = open('.git_key_file', 'r').read().strip()

# Derived names
dqdb_base_name = options.get("name", "velo_multi_publish")
provider = options.get("provider", "RecoMon")
saveset_dir = f"/hist/Savesets/ByRun/{provider}/"
file_prefix = provider + "-run"

# Validate options minimal keys
required_keys = ["name", "locations", "method"]
for k in required_keys:
    if k not in options:
        print(f"Options file missing required key: {k}")
        sys.exit(1)

# Prepare storage for published data
published_data_for_verification = {
    sensor: {
        module_idx: {"runs": [], "values": [], "errors": []}
        for module_idx in range(TOTAL_MODULES)
    } for sensor in VELO_SENSOR_NAMES
}

runs_info = {"numbers": [], "length": []}

# Main loop over runs
for run in range(run_lower, run_upper + 1):
    run_filename = saveset_dir + run_dir_from_run_no(run) + file_prefix + str(run) + file_suffix
    run_info_file = f"{rundb_info}/{run}"

    if not os.path.exists(run_info_file):
        proc = subprocess.Popen(f"wget {rundb_loc}{run} -P {rundb_info}",
                                shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        proc.communicate()

    try:
        run_info_dict = json.load(open(run_info_file))
    except Exception as e:
        print(f"Run {run}: cannot read run info ({e}), skipping")
        continue

    if run_info_dict.get('state') == 'CREATED':
        continue
    if require_offline and run_info_dict.get('destination') != 'OFFLINE':
        continue

    try:
        run_start = time.mktime(time.strptime(run_info_dict['starttime'], rundb_time_format))
        run_end = time.mktime(time.strptime(run_info_dict['endtime'], rundb_time_format))
        run_length = run_end - run_start
    except Exception as e:
        print(f"Run {run}: run time parse error ({e}), skipping")
        continue

    if run_length < min_run_length:
        continue

    if not os.path.exists(run_filename):
        print(f"Run {run}: saveset missing, attempting to create via monitoringhub")
        proc = subprocess.Popen(
            'wget http://monitoringhub.lbdaq.cern.ch/v1/createrunsaveset/LHCb/' + str(run) +
            '?date=' + run_info_dict['starttime'][0:10] + '\&path=/hist',
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proc.communicate()
        if not os.path.exists(run_filename):
            print(f"Run {run}: saveset still missing after request, skipping")
            continue

    run_file = TFile.Open(run_filename)
    if not run_file or run_file.IsZombie():
        print(f"Run {run}: cannot open saveset file, skipping")
        continue
    if run_file.TestBit(TFile.kRecovered):
        print(f"Run {run}: file needed recovery, skipping")
        continue

    runs_info["numbers"].append(run)
    runs_info["length"].append(run_length / 3600.0)

    # --- Loop through each sensor and its modules ---
    for sensor_name in VELO_SENSOR_NAMES:
        # FIXED: only replace the histogram name token, not every "VP"
        location = options["locations"][0].replace("hiteff_asicVP", f"hiteff_asic{sensor_name}")
        hist = run_file.Get(location)
        if not hist:
            print(f"Run {run}: histogram '{location}' not found, skipping sensor {sensor_name}")
            continue

        for module_idx in range(TOTAL_MODULES):
            bin_number = module_idx + 1
            if bin_number < 1 or bin_number > hist.GetNbinsX():
                print(f"Run {run}: Sensor {sensor_name}, module {module_idx} -> invalid bin {bin_number}, skipping")
                continue

            eff = hist.GetBinContent(bin_number)
            eff_err = hist.GetBinError(bin_number)

            metric_name = f"velo_asic_{sensor_name}_mod{module_idx}_eff"
            metric_err_name = metric_name + "_err"
            unique_algorithm = f"rta_piquet_trends|tracking|publish_velo_{sensor_name}_mod{module_idx}"

            data_to_publish = {
                metric_name: eff,
                metric_err_name: eff_err,
            }

            published_data_for_verification[sensor_name][module_idx]["runs"].append(run)
            published_data_for_verification[sensor_name][module_idx]["values"].append(eff)
            published_data_for_verification[sensor_name][module_idx]["errors"].append(eff_err)

            if publish_to_dqdb:
                try:
                    save_in_dqdb(git_key, run, '', data_to_publish, metric_name, unique_algorithm)
                    print(f"Run {run}: published sensor {sensor_name}, module {module_idx}")
                except Exception as e:
                    print(f"Run {run}: save_in_dqdb FAILED for sensor {sensor_name}, module {module_idx}: {e}")

# After loop: create verification overlay PNGs
out_dir = Path(f"{Path.cwd()}/figures/{run_lower}_{run_upper}/publishing_all_sensors_verify/")
out_dir.mkdir(parents=True, exist_ok=True)

for sensor_name in VELO_SENSOR_NAMES:
    any_points_for_sensor = any(published_data_for_verification[sensor_name][m]["runs"]
                                for m in range(TOTAL_MODULES))
    if any_points_for_sensor:
        png_path = out_dir / f"velo_asic_{sensor_name}_eff_{run_lower}_{run_upper}_all_modules_verify.png"
        canvas = TCanvas(f"c_verify_{sensor_name}", f"Verification - {sensor_name} all modules", 1400, 700)
        canvas.SetGrid()
        mg = TMultiGraph()
        legend = TLegend(0.75, 0.15, 0.95, 0.55)
        legend.SetFillStyle(0)
        legend.SetBorderSize(0)

        colors = [ROOT.kBlue, ROOT.kRed, ROOT.kGreen+2, ROOT.kMagenta, ROOT.kCyan,
                  ROOT.kOrange+7, ROOT.kViolet-5, ROOT.kSpring+9, ROOT.kAzure+10,
                  ROOT.kBlack, ROOT.kGray, ROOT.kPink, ROOT.kSpring, ROOT.kTeal,
                  ROOT.kViolet, ROOT.kOrange, ROOT.kCyan+2, ROOT.kRed-4, ROOT.kGreen-3,
                  ROOT.kMagenta-2, ROOT.kBlue-1, ROOT.kYellow+2, ROOT.kSpring+10,
                  ROOT.kAzure+9, ROOT.kPink+1, ROOT.kOrange-3, ROOT.kCyan-1, ROOT.kRed+1,
                  ROOT.kGreen+1, ROOT.kMagenta+1, ROOT.kBlue+1, ROOT.kYellow-1, ROOT.kSpring-1,
                  ROOT.kAzure-1, ROOT.kPink-1, ROOT.kOrange+1, ROOT.kCyan+1, ROOT.kRed+2,
                  ROOT.kGreen+3, ROOT.kMagenta+4, ROOT.kBlue+5, ROOT.kYellow+6, ROOT.kSpring+7,
                  ROOT.kAzure+8, ROOT.kPink+9, ROOT.kOrange+10, ROOT.kCyan+11, ROOT.kRed+12,
                  ROOT.kGreen+13, ROOT.kMagenta+14, ROOT.kBlue+15, ROOT.kYellow+16]

        for idx, module_idx in enumerate(range(TOTAL_MODULES)):
            data = published_data_for_verification[sensor_name][module_idx]
            if not data["runs"]:
                continue
            order = np.argsort(data["runs"])
            x_vals = np.array(data["runs"], dtype='f')[order]
            y_vals = np.array(data["values"], dtype='f')[order]
            y_errs = np.array(data["errors"], dtype='f')[order]
            n = len(x_vals)
            x_errs = np.zeros(n, dtype='f')

            graph = TGraphErrors(n, x_vals, y_vals, x_errs, y_errs)
            graph.SetMarkerStyle(20)
            graph.SetMarkerSize(0.9)
            color = colors[idx % len(colors)]
            graph.SetMarkerColor(color)
            graph.SetLineColor(color)
            mg.Add(graph, "P")
            legend.AddEntry(graph, f"Mod {module_idx}", "p")

        mg.SetTitle(f"Hit Efficiency ({sensor_name}) - All Modules")
        mg.Draw("AP")
        if mg.GetXaxis():
            mg.GetXaxis().SetTitle("Run Number")
            mg.GetXaxis().SetNoExponent(True)
            mg.GetXaxis().SetNdivisions(505)
        if mg.GetYaxis():
            mg.GetYaxis().SetTitle("Hit Efficiency")

        legend.Draw()
        canvas.Update()
        canvas.SaveAs(str(png_path))
        print(f"Saved verification plot: {png_path}")
    else:
        print(f"No published points found for sensor {sensor_name}; no verification PNG created.")

total_published_points = sum(len(published_data_for_verification[s][m]["runs"])
                              for s in VELO_SENSOR_NAMES for m in range(TOTAL_MODULES))
print(f"Finished. Total unique sensor-module points published: {total_published_points}")

sys.exit(0)
