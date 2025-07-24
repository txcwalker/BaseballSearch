# fangraphs_concat

import pandas as pd
import glob

# Merge all batting files
batting_files = glob.glob("../data/fangraphs/batting_by_year/*.csv")
batting_all = pd.concat((pd.read_csv(f) for f in batting_files), ignore_index=True)

# Merge all pitching files
pitching_files = glob.glob("../data/fangraphs/pitching_by_year/*.csv")
pitching_all = pd.concat((pd.read_csv(f) for f in pitching_files), ignore_index=True)

# Save if needed (optional)
batting_all.to_csv("../data/fangraphs/fangraphs_batting_merged.csv", index=False)
pitching_all.to_csv("../data/fangraphs/fangraphs_pitching_merged.csv", index=False)

