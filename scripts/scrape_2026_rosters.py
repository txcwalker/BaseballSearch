import requests
import pandas as pd

# Mapping MLB Stats API abbreviations to our DB standards
ABBR_MAP = {
    'AZ': 'ARI', 'TB': 'TBR', 'WSH': 'WSN', 'CWS': 'CHW', 
    'KC': 'KCR', 'SD': 'SDN', 'SF': 'SFG', 'OAK': 'ATH'
}

def get_rosters():
    print("Fetching MLB rosters via Stats API...")
    
    # 1. Get all teams
    teams_r = requests.get('https://statsapi.mlb.com/api/v1/teams?sportId=1')
    teams_data = teams_r.json()['teams']
    
    all_rosters = []
    
    for team in teams_data:
        team_id = team['id']
        raw_abbr = team['abbreviation']
        # Apply mapping or use raw
        abbr = ABBR_MAP.get(raw_abbr, raw_abbr)
        
        # 2. Get roster
        roster_url = f'https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=40Man'
        try:
            roster_r = requests.get(roster_url, timeout=10)
            players = roster_r.json().get('roster', [])
            for p in players:
                all_rosters.append({
                    'player_id': p['person']['id'],
                    'name': p['person']['fullName'],
                    'team': abbr,
                    'year': 2026
                })
            print(f"  {abbr}: Found {len(players)} players")
        except Exception as e:
            print(f"  Error fetching {abbr}: {e}")
            
    df = pd.DataFrame(all_rosters)
    df.to_csv('rosters_2026_bridge.csv', index=False)
    print(f"Saved {len(df)} roster entries to rosters_2026_bridge.csv")

if __name__ == "__main__":
    get_rosters()
