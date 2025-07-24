-- Fangraphs Batting Tables

-- Fangraphs Batting: Lahman-like Table
CREATE TABLE fangraphs_batting_lahman_like (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    g INT,
    ab INT,
    pa INT,
    h INT,
    singles INT,
    doubles INT,
    triples INT,
    hr INT,
    r INT,
    rbi INT,
    bb INT,
    ibb INT,
    so INT,
    hbp INT,
    sf INT,
    sh INT,
    sb INT,
    cs INT,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Batting: Standard Metrics
CREATE TABLE fangraphs_batting_standard (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    avg NUMERIC,
    obp NUMERIC,
    slg NUMERIC,
    ops NUMERIC,
    iso NUMERIC,
    babip NUMERIC,
    "bb_pc" NUMERIC,
    "k_pc" NUMERIC,
    "bb_k" NUMERIC,
    gdp INT,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Batting: Advanced Metrics
CREATE TABLE fangraphs_batting_advanced (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    woba NUMERIC,
    wraa NUMERIC,
    wrc NUMERIC,
    wrc_plus INT,
    war NUMERIC,
    rar NUMERIC,
    bat NUMERIC,
    fld NUMERIC,
    rep NUMERIC,
    pos NUMERIC,
    off NUMERIC,
    def NUMERIC,
    dol NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Plate Discipline
CREATE TABLE fangraphs_plate_discipline (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    "o_swing_pc" NUMERIC,
    "z_swing_pc" NUMERIC,
    "swing_pc" NUMERIC,
    "o_contact_pc" NUMERIC,
    "z_contact_pc" NUMERIC,
    "contact_pc" NUMERIC,
    "zone_pc" NUMERIC,
    "f_strike_pc" NUMERIC,
    "swstr_pc" NUMERIC,
    cstr_pc NUMERIC,
    csw_pc NUMERIC,
    wpa NUMERIC,
    wpa_li NUMERIC,
    clutch NUMERIC,
    re24 NUMERIC,
    rew NUMERIC,
    pli NUMERIC,
    phli NUMERIC,
    ph INT,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Batted Ball
CREATE TABLE fangraphs_batted_ball (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    gb INT,
    fb INT,
    ld INT,
    iffb INT,
    "gb_fb" NUMERIC,
    "ld_pc" NUMERIC,
    "gb_pc" NUMERIC,
    "fb_pc" NUMERIC,
    "iffb_pc" NUMERIC,
    "hr_fb" NUMERIC,
    ifh INT,
    "ifh_pc" NUMERIC,
    bu INT,
    buh INT,
    "buh_pc" NUMERIC,
    "pull_pc" NUMERIC,
    "cent_pc" NUMERIC,
    "oppo_pc" NUMERIC,
    "soft_pc" NUMERIC,
    "med_pc" NUMERIC,
    "hard_pc" NUMERIC,
    hardhit INT,
    "hardhit_pc" NUMERIC,
    ev NUMERIC,
    la NUMERIC,
    barrels INT,
    "barrel_pc" NUMERIC,
    maxev NUMERIC,
    "tto_pc" NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Baserunning & Fielding
CREATE TABLE fangraphs_baserunning_fielding (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    bsr NUMERIC,
    spd NUMERIC,
    wsb NUMERIC,
    ubr NUMERIC,
    wgdp NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Batter Pitch Type Summary
CREATE TABLE fangraphs_batter_pitch_type_summary (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    fb_pc NUMERIC,
    fbv NUMERIC,
    sl_pc NUMERIC,
    slv NUMERIC,
    ch_pc NUMERIC,
    chv NUMERIC,
    cb_pc NUMERIC,
    cbv NUMERIC,
    sf_pc NUMERIC,
    sfv NUMERIC,
    ct_pc NUMERIC,
    ctv NUMERIC,
    kn_pc NUMERIC,
    knv NUMERIC,
    xx_pc NUMERIC,
    po_pc NUMERIC,
    wfb NUMERIC,
    wsl NUMERIC,
    wch NUMERIC,
    wcb NUMERIC,
    wsf NUMERIC,
    wct NUMERIC,
    wkn NUMERIC,
    wfb_c NUMERIC,
    wsl_c NUMERIC,
    wch_c NUMERIC,
    wcb_c NUMERIC,
    wsf_c NUMERIC,
    wct_c NUMERIC,
    wkn_c NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Pitching Tables

-- Fangraphs Lahman Pitching Table
CREATE TABLE fangraphs_pitching_lahman_like (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    w INT,
    l INT,
    g INT,
    gs INT,
    cg INT,
    sho INT,
    sv INT,
    ip NUMERIC,
    h INT,
    r INT,
    er INT,
    hr INT,
    bb INT,
    so INT,
    hbp INT,
    wp INT,
    bk INT,
    tbf INT,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Standard Pitching
CREATE TABLE fangraphs_pitching_standard (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    era NUMERIC,
    k_9 NUMERIC,
    bb_9 NUMERIC,
    k_bb NUMERIC,
    h_9 NUMERIC,
    hr_9 NUMERIC,
    avg NUMERIC,
    whip NUMERIC,
    babip NUMERIC,
    lob_pc NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Advanced Pitching
CREATE TABLE fangraphs_pitching_advanced (
    id INT,
    season INT,
    "name" TEXT,
    team TEXT,
    war NUMERIC,
    fip NUMERIC,
    xfip NUMERIC,
    siera NUMERIC,
    era_minus NUMERIC,
    fip_minus NUMERIC,
    xfip_minus NUMERIC,
    rar NUMERIC,
    dollars NUMERIC,
    ra9_war NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Plate Discipline
CREATE TABLE fangraphs_pitching_plate_discipline (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    o_swing_pc NUMERIC,
    z_swing_pc NUMERIC,
    swing_pc NUMERIC,
    o_contact_pc NUMERIC,
    z_contact_pc NUMERIC,
    contact_pc NUMERIC,
    zone_pc NUMERIC,
    f_strike_pc NUMERIC,
    swstr_pc NUMERIC,
    cstr_pc NUMERIC,
    csw_pc NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Pitching Batted Ball
CREATE TABLE fangraphs_pitching_batted_ball (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    gb_fb NUMERIC,
    ld_pc NUMERIC,
    gb_pc NUMERIC,
    fb_pc NUMERIC,
    iffb_pc NUMERIC,
    hr_fb NUMERIC,
    pull_pc NUMERIC,
    cent_pc NUMERIC,
    oppo_pc NUMERIC,
    soft_pc NUMERIC,
    med_pc NUMERIC,
    hard_pc NUMERIC,
    ev NUMERIC,
    la NUMERIC,
    barrels INT,
    barrel_pc NUMERIC,
    maxev NUMERIC,
    hardhit INT,
    hardhit_pc NUMERIC,
    tto_pc NUMERIC,
    PRIMARY KEY (idfg, season)
);

-- Fangraphs Pitching Pitch Type
CREATE TABLE fangraphs_pitching_pitch_type_summary (
    idfg INT,
    season INT,
    "name" TEXT,
    team TEXT,
    fb_pc NUMERIC,
    fbv NUMERIC,
    sl_pc NUMERIC,
    slv NUMERIC,
    ct_pc NUMERIC,
    ctv NUMERIC,
    cb_pc NUMERIC,
    cbv NUMERIC,
    ch_pc NUMERIC,
    chv NUMERIC,
    sf_pc NUMERIC,
    sfv NUMERIC,
    kn_pc NUMERIC,
    knv NUMERIC,
    xx_pc NUMERIC,
    po_pc NUMERIC,
    wfb NUMERIC,
    wsl NUMERIC,
    wct NUMERIC,
    wcb NUMERIC,
    wch NUMERIC,
    wsf NUMERIC,
    wkn NUMERIC,
    wfb_c NUMERIC,
    wsl_c NUMERIC,
    wct_c NUMERIC,
    wcb_c NUMERIC,
    wch_c NUMERIC,
    wsf_c NUMERIC,
    wkn_c NUMERIC,
    PRIMARY KEY (idfg, season)
);


