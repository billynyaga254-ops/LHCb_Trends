import ROOT
from ROOT import TH1F, TCanvas, TFile
import os,sys
from math import floor, sqrt
import subprocess
import json
import time
from importlib import import_module as load_options
from pathlib import Path
from save_in_dqdb import save_in_dqdb

def run_dir_from_run_no(run_no):
    """
    We have to figure out the relevant subdirectories.
    The recomon folders are organised as 260000/265000/RecoMon-run265911.root
    where the first two subdirectories come from the run number
    Since the start and end run numbers can span subdirectories
    we need a helper function to generate this from the run number
    """
    sd_1 = floor(run_no/10000)*10000
    sd_2 = floor(run_no/1000)*1000
    return str(sd_1)+"/"+str(sd_2)+"/"

def entries_selection(histogram, selection_type):
    """
    Helper function to deal with multi-dimensional histograms
    """
    is2D = histogram.InheritsFrom("TH2")
    if selection_type in ["quadrant1", "quadrant2", "quadrant3", "quadrant4"] and not is2D:
        print("entries_selection():",histogram.GetName(),"is not 2D histogram, choose a different method")
        sys.exit(0)
    zero_x = histogram.FindBin(0)
    zero_y = zero_x
    if is2D:
        zero_x = histogram.ProjectionX().FindBin(0)
        zero_y = histogram.ProjectionY().FindBin(0)
    match selection_type:
        case "negative":
            if is2D:
                return histogram.Integral(1,zero_x,1,histogram.GetNbinsY())
            else:
                return histogram.Integral(1,zero_x)
        case "positive":
            if is2D:
                return histogram.Integral(zero_x,histogram.GetNbinsX(),1,histogram.GetNbinsY())
            else:
                return histogram.Integral(zero_x,histogram.GetNbinsX())
        case "quadrant1":
            return histogram.Integral(zero_x,histogram.GetNbinsX(),zero_y,histogram.GetNbinsY())
        case "quadrant2":
            return histogram.Integral(1,zero_x,zero_y,histogram.GetNbinsY())
        case "quadrant3":
            return histogram.Integral(1,zero_x,1,zero_y)
        case "quadrant4":
            return histogram.Integral(zero_x,histogram.GetNbinsX(),1,zero_y)
        case _:
            print("entries_selection(): selection type",selection_type,"unknown")
            sys.exit(0)

"""
End helper functions and begin main body of script
We begin by deriving a bunch of locations and other "constants"
from various filesystem paths and command line inputs. This
could be made more configurable in the future.
"""

ROOT.gROOT.SetBatch(True)

rundb_loc   = 'http://rundb-internal.lbdaq.cern.ch/api/run/'
rundb_info  = 'rundb_files'
rundb_time_format = '%Y-%m-%dT%H:%M:%S%z'
file_suffix = '.root'
min_run_length = 300. # minimum length of runs to be analyzed, in seconds
require_offline = False # Look at data taken with destination local if needed
publish_to_dqdb = True # Should we publish the information to the DQDB for trend plotting in MONET?

if len(sys.argv) < 4:
    print("Lower and upper run number ranges must be specified, as well as the options")
    sys.exit(0)

report_type = sys.argv[4] if len(sys.argv) == 5 else "unspecified"

run_lower = int(sys.argv[1])
run_upper = int(sys.argv[2])
if run_upper < run_lower :
    print("The upper run number must be higher than the lower run number")
    sys.exit(0)

try:
    algorithm = "rta_piquet_trends|"+sys.argv[3].replace('/','|') # For publishing to DQDB
    options_module = load_options(f"options.{sys.argv[3].replace('/','.')}")
    options = options_module.options()
    # Check if a hotspot dictionary exists in the options file, and load it if it does
    if hasattr(options_module, 'populated_regions'):
        populated_regions = options_module.populated_regions
    else:
        populated_regions = {} # Define an empty dict if this options file doesn't use it
except Exception as e:
    print(f"Error loading options module: {e}")
    sys.exit(1)


"""
Now set the info for publishing to DQDB
The git key is hardcoded here to avoid committing it to the repo
Maybe there is a more elegant way to do this?
"""
git_key = open('.git_key_file','r').read().rstrip('\n')
dqdb_name = options["name"]
dqdb_err_name = options["name"]+"_err"

# Verify the options have the expected keys
required_keys = ["name","type","locations","errors",
                 "method","counts","y_axis_title"]
if False in [i in options for i in required_keys]:
    print("Provided options are missing a required key, exiting")
    print("Required keys are",required_keys)
    print("Provided keys were",options)
    sys.exit(1)

# Optional key to specify a provider which is not RecoMon
provider = "RecoMon"
if "provider" in options:
    provider = options["provider"]

saveset_dir = '/hist/Savesets/ByRun/'+provider+'/'
file_prefix = provider+'-run'

"""
Now we get into the main body of the script, which is actually
going to calculate the relevant quantities and populate the
histograms (as well as the DQDB if that is requested)
"""

runs_info = {"numbers": [], "length": []}
for run in range(run_lower,run_upper+1):
    # The upper and lower run numbers are inclusive
    run_filename = saveset_dir+run_dir_from_run_no(run)+file_prefix+str(run)+file_suffix
    # Check if we downloaded the runDB info for this run already
    run_info_file = rundb_info+'/'+str(run)
    if not os.path.exists(run_info_file):
        process = subprocess.Popen(
            'wget '+rundb_loc+str(run)+' -P '+rundb_info,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            shell = True
        )
        std_out, std_err = process.communicate()

    """
    Does this run have the right destination?
    It is a bit lazy to use destination OFFLINE as a proxy for
    "we are in global with all relevant detectors" but this whole
    script is agricultural so why make it fancier...
    """
    run_info_dict = json.loads(open(run_info_file).read())
    try:
        if run_info_dict['state'] == 'CREATED' :
            continue
    except:
        print('Run',run,'does not have a valid run info dictionary, continuing')
        continue
    if require_offline and run_info_dict['destination'] != 'OFFLINE':
        continue
    if not os.path.exists(run_filename) :
        print("Saveset for run "+str(run)+" with destination OFFLINE does not exist, trying to create it")
        process = subprocess.Popen(
            'wget http://monitoringhub.lbdaq.cern.ch/v1/createrunsaveset/LHCb/'+\
                   str(run)+'?date='+run_info_dict['starttime'][0:10]+'\&path=/hist',
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            shell = True
        )
        std_out,std_err = process.communicate()
        if not os.path.exists(run_filename) :
            print("Saveset for run "+str(run)+" with destination OFFLINE still does not exist, ignoring it")
            continue

    # Get the run length, used to normalise yields
    run_start = time.mktime(time.strptime(run_info_dict['starttime'],rundb_time_format))
    run_end   = time.mktime(time.strptime(run_info_dict['endtime'],rundb_time_format))
    run_length = run_end-run_start

    # Ignore runs below 5 minutes in length
    if run_length < min_run_length :
        print("Run "+str(run)+" was requested but is shorter than "+str(min_run_length)+" seconds, skipping")
        continue

    # Now extract the actual information
    run_file = TFile.Open(run_filename)
    # Ignore runs with file not properly closed
    if run_file.TestBit(TFile.kRecovered):
        print("Run "+str(run)+" file needed to recovered and it can crash, skipping")
        continue

    runs_info["numbers"].append(run)
    runs_info["length"].append(run_length/3600.) # convert seconds to hours

    # For now support two kinds of information: absolute values and ratios
    temp_counts = []
    temp_errors = []
    # First check that all requested locations are actually present in the file
    location_not_found = False
    for i,location in enumerate(options["locations"]):
        run_object = run_file.Get(location)
        if not run_object:
            print("Run "+str(run)+" does not contain "+location+" information")
            location_not_found = True
            break
    if location_not_found:
        continue

    for i,location in enumerate(options["locations"]):
        run_object = run_file.Get(location)
        # Without background subtraction or (to be implemented) with
        if options["method"][i] == "raw" :
            raw_count = run_object.GetEntries()
            this_count = raw_count/runs_info["length"][-1]
            this_count_err = sqrt(raw_count)/runs_info["length"][-1]
        elif options["method"][i] == "mean" :
            this_count = run_object.GetMean(1)
            this_count_err = run_object.GetMeanError(1)
        elif options["method"][i] == "RMS" :
            this_count = run_object.GetRMS(1)
            this_count_err = run_object.GetRMSError(1)
        elif options["method"][i] in ["quadrant1", "quadrant2", "quadrant3", "quadrant4", "negative", "positive"]:
            raw_count = entries_selection(run_object, options["method"][i])
            this_count = raw_count/runs_info["length"][-1]
            this_count_err = temp_counts[-1]/sqrt(raw_count)
        # ===================================================================
        # NEW METHOD FOR CALCULATING MEAN OCCUPANCY IN A DEFINED 2D HOTSPOT
        # ===================================================================
        elif options["method"][i] == "hotspot_mean":
            # Get the short name of the sensor to use as a dictionary key
            # e.g., "VPClusterMonitors_BeamCrossing/VPClusterMapOnMod10Sens2" -> "Mod10Sens2"
            sensor_key = run_object.GetName().replace("VPClusterMapOn", "")

            # Look up the region of interest (ROI) coordinates from our dictionary
            if sensor_key not in populated_regions:
                print(f"Warning: Sensor {sensor_key} not found in populated_regions dictionary. Skipping.")
                this_count = 0
                this_count_err = 0
            else:
                roi = populated_regions[sensor_key]
                # Check if the region is defined (i.e., not for a scattered sensor)
                if not roi['x'] or not roi['y']:
                    this_count = 0
                    this_count_err = 0
                else:
                    # Convert the axis values from our dictionary into the correct bin indices
                    start_x_bin = run_object.GetXaxis().FindBin(roi['x'][0])
                    end_x_bin   = run_object.GetXaxis().FindBin(roi['x'][1])
                    start_y_bin = run_object.GetYaxis().FindBin(roi['y'][0])
                    end_y_bin   = run_object.GetYaxis().FindBin(roi['y'][1])

                    # Calculate the total number of bins in our rectangular region
                    n_bins_in_roi = (end_x_bin - start_x_bin + 1) * (end_y_bin - start_y_bin + 1)

                    # Get the sum of all entries within the hotspot bins
                    integral = run_object.Integral(start_x_bin, end_x_bin, start_y_bin, end_y_bin)

                    if n_bins_in_roi > 0 and integral > 0:
                        # The mean is the total count divided by the number of bins
                        mean_occupancy = integral / n_bins_in_roi

                        # The error is sqrt(total_count) / number_of_bins
                        mean_occupancy_err = sqrt(integral) / n_bins_in_roi

                        # Normalize to the run length in hours
                        this_count = mean_occupancy / runs_info["length"][-1]
                        this_count_err = mean_occupancy_err / runs_info["length"][-1]
                    else:
                        this_count = 0
                        this_count_err = 0
        elif "bin" in options["method"][i]:
            """
            This can be just "binx" (which will be the bin content normalised to run length)
            or it can be binxbiny (so ratio of bin contents x/y)
            """
            binslist = options["method"][i].split("bin")
            bin = int(binslist[1])
            raw_count = run_object.GetBinContent(bin)
            if len(binslist)>2:
                ref_count = run_object.GetBinContent(int(binslist[2]))
                this_count = raw_count/ref_count
                this_count_err = sqrt((1/raw_count+1/ref_count)*raw_count/ref_count)
            else:
                this_count = raw_count/runs_info["length"][-1]
                this_count_err = temp_counts[-1]/sqrt(raw_count)
        # Append count and error for local histogram plotting
        temp_counts.append(this_count)
        temp_errors.append(this_count_err)

    if options["type"] == "absolute" :
        options["counts"].append(temp_counts[0])
        options["errors"].append(temp_errors[0])
        # Now write to DQDB for publication in MONET
        if publish_to_dqdb :
            save_in_dqdb(git_key, run, '',
                         {dqdb_name : temp_counts[0],
                          dqdb_err_name : temp_errors[0] },
                         options["name"], algorithm )
    elif options["type"] == "ratio" :
        if temp_counts[0] == 0 or temp_counts[1] == 0:
            # Unphysical so do not publish in DQDB
            options["counts"].append(0.)
            options["errors"].append(0.)
            continue
        this_ratio = temp_counts[0]/(1.0*temp_counts[1])
        options["counts"].append(this_ratio)
        this_err = this_ratio*sqrt(pow(temp_errors[0]/temp_counts[0],2) + \
                                   pow(temp_errors[1]/temp_counts[1],2))
        options["errors"].append(this_err)
        # Now write to DQDB for publication in MONET
        if publish_to_dqdb :
            save_in_dqdb(git_key, run, '',
                         {dqdb_name : this_ratio,
                          dqdb_err_name : this_err },
                         options["name"], algorithm )


"""
Now plot the results localy.
This is useful even if we publish to DQDB as it can be a more
efficient way to make a bunch of plots in a format ready to be
dropped into a presentation compared with screenshotting MONET.
"""

output_plot = TH1F(options["name"].lower(),options["name"],
                   len(runs_info["numbers"]),
                   0,len(runs_info["numbers"]))
output_plot.GetXaxis().SetTitle("Run")
output_plot.GetXaxis().SetLabelSize(0.03)
output_plot.GetYaxis().SetTitleSize(0.04)
output_plot.GetYaxis().SetTitle(options["y_axis_title"])
output_plot.GetYaxis().SetTitleOffset(1.35)
for i,entry in enumerate(options["counts"]) :
    output_plot.SetBinContent(i+1,entry)
    output_plot.SetBinError(i+1,options["errors"][i])
    output_plot.GetXaxis().SetBinLabel(i+1,str(runs_info["numbers"][i]))
output_plot.LabelsOption("v","X")

y_range = options.get("y_range",None)
if y_range is not None:
    output_plot.SetMinimum(y_range[0])
    output_plot.SetMaximum(y_range[1])
else:
    output_plot_minimum = output_plot.GetMinimum()
    """
    Now for a bit of tortured logic to remove upper outliers in some plots...
    """
    output_plot_maximum = 0.
    for thisbin in range(output_plot.GetNbinsX()):
        output_plot_maximum += output_plot.GetBinContent(thisbin+1)
    output_plot_maximum /= 1.0*output_plot.GetNbinsX()
    if 5.*output_plot_maximum > output_plot.GetMaximum():
        output_plot_maximum = output_plot.GetMaximum()
    #
    if output_plot_minimum > 0. :
        output_plot.GetYaxis().SetRangeUser(0,1.5*output_plot_maximum)
    else :
        """
        The earlier outlier removal does not really work if you are oscillating
        around zero since you finish with a very small mean value
        """
        output_plot_maximum = output_plot.GetMaximum()
        if output_plot_maximum > 0. :
            output_plot.GetYaxis().SetRangeUser(1.5*output_plot_minimum,1.5*output_plot_maximum)
        else :
            output_plot.GetYaxis().SetRangeUser(1.5*output_plot_minimum,0.5*output_plot_maximum)

canvas = TCanvas("trend_canvas","trend_canvas",1200,800)
canvas.cd()
output_plot.Draw()
plotname=f"{Path.cwd()}/figures/{run_lower}_{run_upper}/{report_type}/"+options["name"]+f"_{run_lower}_{run_upper}.png"
plotdirectory=plotname.rpartition('/')[0]
if Path(plotdirectory).exists() is False:
    Path(plotdirectory).mkdir(parents=True, exist_ok=True)
canvas.SaveAs(plotname)
canvas.SaveAs(plotname.replace('png','pdf'))
