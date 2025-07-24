-- Postgres DDL for tables (if typed manually) --

-- db/schema_lahman.sql (complete typed SQL schema for Lahman database)

-- NOTE:
-- This schema uses natural composite primary keys wherever possible.
-- The Lahman dataset provides stable, well-defined identifiers (e.g., playerID, yearID, stint),
-- which are sufficient for uniquely identifying rows and maintaining data integrity.
-- This approach prioritizes transparency, readability, and alignment with existing data structures.
-- If future needs require integration with external systems or more flexible indexing,
-- UUIDs could be introduced as surrogate keys alongside existing composite keys.
-- Double Plays are gidp here and gdp in fangraphs tables


-- PEOPLE TABLE
CREATE TABLE people (
    playerid TEXT PRIMARY KEY,
    birthyear INT,
    birthmonth INT,
    birthday INT,
    birthcountry TEXT,
    birthstate TEXT,
    birthcity TEXT,
    deathyear INT,
    deathmonth INT,
    deathday INT,
    deathcountry TEXT,
    deathstate TEXT,
    deathcity TEXT,
    namefirst TEXT,
    namelast TEXT,
    namegiven TEXT,
    weight INT,
    height INT,
    bats TEXT,
    throws TEXT,
    debut DATE,
    finalgame DATE,
    retroid TEXT,
    bbrefid TEXT
);

-- TEAMS FRANCHISES TABLE
CREATE TABLE teamsfranchises (
    franchid TEXT PRIMARY KEY,
    franchname TEXT,
    active CHAR(2),
    naassoc TEXT
);

-- TEAMS TABLE
CREATE TABLE teams (
    yearid INT,
    lgid TEXT,
    teamid TEXT,
    franchid TEXT,
    divid TEXT,
    rank INT,
    g INT,
    ghome INT,
    w INT,
    l INT,
    divwin CHAR(1),
    wcwin CHAR(1),
    lgwin CHAR(1),
    wswin CHAR(1),
    r INT,
    ab INT,
    h INT,
    doubles INT,
    triples INT,
    hr INT,
    bb INT,
    so INT,
    sb INT,
    cs INT,
    hbp INT,
    sf INT,
    ra INT,
    er INT,
    era NUMERIC(4,2),
    cg INT,
    sho INT,
    sv INT,
    ipouts INT,
    ha INT,
    hra INT,
    bba INT,
    soa INT,
    e INT,
    dp INT,
    fp NUMERIC(4,3),
    "name" TEXT,
    park TEXT,
    attendance INT,
    bpf INT,
    ppf INT,
    teamidbr TEXT,
    teamidlahman45 TEXT,
    teamidretro TEXT,
    PRIMARY KEY (yearid, teamid)
);

-- PARKS TABLE
CREATE TABLE parks (
    parkkey TEXT PRIMARY KEY,
    parkname TEXT,
    parkalias TEXT,
    city TEXT,
    state TEXT,
    country TEXT
);

-- BATTING TABLE
CREATE TABLE batting (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    stint INT,
    teamid TEXT,
    lgid TEXT,
    g INT,
    ab INT,
    r INT,
    h INT,
    doubles INT,
    triples INT,
    hr INT,
    rbi INT,
    sb INT,
    cs INT,
    bb INT,
    so INT,
    ibb INT,
    hbp INT,
    sh INT,
    sf INT,
    gidp INT,
    PRIMARY KEY (playerid, yearid, stint)
);

-- PITCHING TABLE
CREATE TABLE pitching (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    stint INT,
    teamid TEXT,
    lgid TEXT,
    w INT,
    l INT,
    g INT,
    gs INT,
    cg INT,
    sho INT,
    sv INT,
    ipouts INT,
    h INT,
    er INT,
    hr INT,
    bb INT,
    so INT,
    baopp NUMERIC(5,3),
    era NUMERIC(6,2),
    ibb INT,
    wp INT,
    hbp INT,
    bk INT,
    bfp INT,
    gf INT,
    r INT,
    sh INT,
    sf INT,
    gidp INT,
    PRIMARY KEY (playerid, yearid, stint)
);

-- FIELDING TABLE
CREATE TABLE fielding (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    stint INT,
    teamid TEXT,
    lgid TEXT,
    pos TEXT,
    g INT,
    gs INT,
    innouts INT,
    po INT,
    a INT,
    e INT,
    dp INT,
    pb INT,
    wp INT,
    sb INT,
    cs INT,
    zr NUMERIC(6,3),
    PRIMARY KEY (playerid, yearid, stint, pos)
);

-- FIELDINGOF TABLE
CREATE TABLE fieldingof (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    stint INT,
    glf INT,
    gcf INT,
    grf INT,
    PRIMARY KEY (playerid, yearid, stint)
);

-- FIELDINGOF SPLIT TABLE
CREATE TABLE fieldingofsplit (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    stint INT,
    teamid TEXT,
    lgid TEXT,
    pos TEXT,
    g INT,
    gs INT,
    innouts INT,
    po INT,
    a INT,
    e INT,
    dp INT,
    pb INT,
    wp INT,
    sb INT,
    cs INT,
    zr NUMERIC (6,3),
    PRIMARY KEY (playerid, yearid, stint, pos)
);

-- APPEARANCES TABLE
CREATE TABLE appearances (
    yearid INT,
    teamid TEXT,
    lgid TEXT,
    playerid TEXT REFERENCES people(playerid),
    g_all INT,
    gs INT,
    g_batting INT,
    g_defense INT,
    g_p INT,
    g_c INT,
    g_1b INT,
    g_2b INT,
    g_3b INT,
    g_ss INT,
    g_lf INT,
    g_cf INT,
    g_rf INT,
    g_of INT,
    g_dh INT,
    g_ph INT,
    g_pr INT,
    PRIMARY KEY (yearid, teamid, playerid)
);

-- MANAGERS TABLE
CREATE TABLE managers (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    teamid TEXT,
    lgid TEXT,
    inseason INT,
    g INT,
    w INT,
    l INT,
    rank INT,
    plyrmgr CHAR(1),
    PRIMARY KEY (playerid, yearid, teamid, inseason)
);

-- ALL STAR FULL TABLE
CREATE TABLE allstarfull (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    gamenum INT,
    gameid TEXT,
    teamid TEXT,
    lgid TEXT,
    gp INT,
    startingpos INT,
    PRIMARY KEY (playerid, yearid, gamenum)
);

-- BATTING POST TABLE
CREATE TABLE battingpost (
    yearid INT,
    round TEXT,
    playerid TEXT REFERENCES people(playerid),
    teamid TEXT,
    lgid TEXT,
    g INT,
    ab INT,
    r INT,
    h INT,
    doubles INT,
    triples INT,
    hr INT,
    rbi INT,
    sb INT,
    cs INT,
    bb INT,
    so INT,
    ibb INT,
    hbp INT,
    sh INT,
    sf INT,
    gidp INT,
    PRIMARY KEY (playerid, yearid, round)
);

-- PITCHING POST TABLE
CREATE TABLE pitchingpost (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    round TEXT,
    teamid TEXT,
    lgid TEXT,
    w INT,
    l INT,
    g INT,
    gs INT,
    cg INT,
    sho INT,
    sv INT,
    ipouts INT,
    h INT,
    er INT,
    hr INT,
    bb INT,
    so INT,
    baopp NUMERIC(5,3),
    era NUMERIC(4,2),
    ibb INT,
    wp INT,
    hbp INT,
    bk INT,
    bfp INT,
    gf INT,
    r INT,
    sh INT,
    sf INT,
    gidp INT,
    PRIMARY KEY (playerid, yearid, round)
);

-- FIELDING POST TABLE
CREATE TABLE fieldingpost (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    teamid TEXT,
    lgid TEXT,
    round TEXT,
    pos TEXT,
    g INT,
    gs INT,
    innouts INT,
    po INT,
    a INT,
    e INT,
    dp INT,
    tp INT,
    pb INT,
    sb INT,
    cs INT,
    PRIMARY KEY (playerid, yearid, round, pos)
);

-- SERIES POST TABLE
CREATE TABLE seriespost (
    yearid INT,
    round TEXT,
    teamidwinner TEXT,
    lgidwinner TEXT,
    teamidloser TEXT,
    lgidloser TEXT,
    wins INT,
    losses INT,
    ties INT,
    PRIMARY KEY (yearid, round)
);

-- HOME GAMES TABLE
CREATE TABLE homegames (
    yearkey INT,
    leaguekey TEXT,
    teamkey TEXT,
    parkkey TEXT,
    spanfirst DATE,
    spanlast DATE,
    games INT,
    openings INT,
    attendance INT,
    PRIMARY KEY (yearkey, teamkey, parkkey)
);

-- MANAGERS HALF TABLE
CREATE TABLE managershalf (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    teamid TEXT,
    lgid TEXT,
    inseason INT,
    half INT,
    g INT,
    w INT,
    l INT,
    rank INT,
    PRIMARY KEY (playerid, yearid, teamid, half)
);

-- TEAMS HALF TABLE
CREATE TABLE teamshalf (
    yearid INT,
    lgid TEXT,
    teamid TEXT,
    half INT,
    divid TEXT,
    divwin CHAR(1),
    rank INT,
    g INT,
    w INT,
    l INT,
    PRIMARY KEY (yearid, teamid, half)
);

-- AWARDS MANAGERS TABLE
CREATE TABLE awardsmanagers (
    playerid TEXT REFERENCES people(playerid),
    awardid TEXT,
    yearid INT,
    lgid TEXT,
    tie CHAR(1),
    notes TEXT,
    PRIMARY KEY (playerid, awardid, yearid)
);

-- AWARDS PLAYERS TABLE
CREATE TABLE awardsplayers (
    playerid TEXT REFERENCES people(playerid),
    awardid TEXT,
    yearid INT,
    lgid TEXT,
    tie CHAR(1),
    notes TEXT,
    PRIMARY KEY (playerid, awardid, yearid)
);

-- AWARDS SHARE MANAGERS TABLE
CREATE TABLE awardssharemanagers (
    awardid TEXT,
    yearid INT,
    lgid TEXT,
    playerid TEXT REFERENCES people(playerid),
    pointswon INT,
    pointsmax INT,
    votesfirst INT,
    PRIMARY KEY (awardid, yearid, playerid)
);

-- AWARDS SHARE PLAYERS TABLE
CREATE TABLE awardsshareplayers (
    awardid TEXT,
    yearid INT,
    lgid TEXT,
    playerid TEXT REFERENCES people(playerid),
    pointswon INT,
    pointsmax INT,
    votesfirst INT,
    PRIMARY KEY (awardid, yearid, playerid)
);

-- HALL OF FAME TABLE
CREATE TABLE halloffame (
    playerid TEXT REFERENCES people(playerid),
    yearid INT,
    votedby TEXT,
    ballots INT,
    needed INT,
    votes INT,
    inducted CHAR(1),
    category TEXT,
    needed_note TEXT,
    PRIMARY KEY (playerid, yearid)
);

-- COLLEGE PLAYING TABLE
CREATE TABLE collegeplaying (
    playerid TEXT REFERENCES people(playerid),
    schoolid TEXT,
    yearid INT,
    PRIMARY KEY (playerid, schoolid, yearid)
);

-- SCHOOLS TABLE
CREATE TABLE schools (
    schoolid TEXT PRIMARY KEY,
    school_name TEXT,
    schoolcity TEXT,
    schoolstate TEXT,
    schoolcountry TEXT
);

-- SALARIES TABLE
CREATE TABLE salaries (
    yearid INT,
    teamid TEXT,
    lgid TEXT,
    playerid TEXT REFERENCES people(playerid),
    salary INT,
    PRIMARY KEY (playerid, yearid)
);

