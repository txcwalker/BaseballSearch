# Baseball Search Tool - Project Roadmap & Tasks

## Completed (May 2026)
- [x] **Database Unification**: Added `playername` and `player_name` columns to all tables to resolve `UndefinedColumn` errors.
- [x] **ETL Boolean Support**: Updated `update_savant_awsrds.py` and `load_all_aws.py` to support `BOOLEAN` types in PostgreSQL.
- [x] **Prompt Hardening**: Hardened `base_prompt_gemini.txt` to prevent `has_tot_row` hallucinations and enforce naming consistency.
- [x] **Schema Documentation**: Detailed FanGraphs table definitions added to `schema_description.txt`.
- [x] **Windows Stability**: Removed emojis and non-ASCII characters from ETL scripts to prevent encoding crashes.

## Ongoing & Future Work
- [ ] **Statcast Integration**: Scale ETL for larger daily Statcast coverage.
- [ ] **Advanced Dashboards**: Implement team-level and season-level summary visualizations in Streamlit.
- [ ] **Template Expansion**: Add more complex SQL templates for niche Sabermetrics.

---

## 🏈 Cross-Project Notes (NFL Simulation)
*Note: Notated here while in the Baseball workspace.*

- [ ] **Advanced Tuning Parameters**: Implement tunable options for:
    - Pace of Play
    - CPOE (Completion Percentage Over Expected)
    - Catch Rate Distributions
    - aDOT (Average Depth of Target)
    - Aggression Index
- [ ] **Team Differentiation**: Ensure these parameters vary by team/roster to move away from "pickem" game outcomes.
