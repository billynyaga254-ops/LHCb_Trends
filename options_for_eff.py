def options():
    return {
        "name": "velo_asic_mods_eff_all_sensors", # A more general name now
        "y_axis_title": "Hit Efficiency (VELO Modules)",
        "provider": "VeloTrackMon",
        "locations": ["VPHitEfficiencyMonitorSensor/hiteff_asicVP"], # This will be dynamically completed in the script
        "method": ["publish_multi_modules"],
        "type": "absolute",
        "counts": [],
    }
