import os
import psycopg2
import logging
import polars as pl
from decimal import Decimal
from datetime import timedelta, datetime, timezone
import io
import sys
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Get and sanitize connection string
connection_string = os.getenv("DATABASE_URL")
if not connection_string:
    logging.error("DATABASE_URL environment variable not set.")
    sys.exit(1)
if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

# Capture UTC timestamp + 2h for processed_at
PROCESS_TS = datetime.now(timezone.utc).replace(microsecond=0)
PROCESSED_AT_PLUS_2H = PROCESS_TS + timedelta(hours=2)
logging.info(
    "Process timestamp (UTC): %s | processed_at (+2h): %s",
    PROCESS_TS.isoformat(), PROCESSED_AT_PLUS_2H.isoformat()
)

# Clean individual row values (fix mixed types from psycopg2)
def clean_row(row, colnames):
    out = {}
    for col, val in zip(colnames, row):
        if isinstance(val, Decimal):
            out[col] = float(val)
            continue
        if col == "date" and isinstance(val, datetime):
            out[col] = val.date()
            continue
        if col == "processed_at":
            if isinstance(val, datetime):
                out[col] = val.replace(microsecond=0).isoformat()
            elif val is None:
                out[col] = None
            else:
                out[col] = str(val)
            continue
        if col == "capital_gains" and val is not None and not isinstance(val, str):
            out[col] = str(val)
            continue
        out[col] = val
    return out

# Generate full date range per ticker (with synthetic rows)
def generate_full_date_range(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame()
    full_data = []
    tickers = df.select("ticker").unique().to_series().to_list()
    for ticker in tickers:
        ticker_df = df.filter(pl.col("ticker") == ticker)
        min_date = ticker_df.select(pl.col("date").min())[0, 0]
        max_date = ticker_df.select(pl.col("date").max())[0, 0]
        days = (max_date - min_date).days + 1
        full_range = pl.DataFrame(
            {
                "date": [min_date + timedelta(days=i) for i in range(days)],
                "ticker": [ticker] * days,
            }
        )
        joined = full_range.join(ticker_df, on=["date", "ticker"], how="left")
        joined = joined.with_columns(
            pl.when(pl.col("open").is_null())
            .then(pl.lit("synthetic"))
            .otherwise(pl.lit("natural"))
            .alias("date_type")
        )
        full_data.append(joined)
    return pl.concat(full_data) if full_data else pl.DataFrame()

# Main script
try:
    with psycopg2.connect(connection_string) as conn:
        schema_name = "raw"
        table_name = "api_data_ingestion_yfinance"
        target_schema = "cdm"
        target_table = "api_data_ingestion_yfinance"
        batch_size = 50
        insert_batch_size = 400_000

        # ---------------------------------------------------------
        # LOAD ALL DISTINCT TICKERS
        # ---------------------------------------------------------
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT ticker
                FROM {schema_name}.{table_name}
                ORDER BY ticker
            """)
            tickers = [row[0] for row in cursor.fetchall()]
            logging.info("Total tickers found: %d", len(tickers))

        # ---------------------------------------------------------
        # OVERRIDE: PROCESS ONLY THESE TICKERS
        # ---------------------------------------------------------

        TICKERS_FULL = ["^GSPC","A","AA","AAL","AAON","AAP","AAPL","AAT","ABBV","ABCB","ABG","ABM","ABNB","ABR",
"ABSI","ABT","ACA","ACAD","ACGL","ACHC","ACHR","ACI","ACIW","ACLS","ACM","ACN","ACT",
"ADEA","ADI","ADM","ADMA","ADNT","ADP","ADPT","ADSK","ADT","ADUS","ADYEN","AEE","AEIS",
"AEO","AEP","AES","AESI","AFG","AFL","AFRM","AGCO","AGNC","AGO","AGYS","AHCO","AHH",
"AIG","AIN","AIR","AIT","AIZ","AJG","AKAM","AKR","AL","ALAB","ALB","ALE","ALEX",
"ALG","ALGM","ALGN","ALGT","ALK","ALKS","ALL","ALLE","ALLY","ALNY","ALRM","ALSN","ALV",
"AM","AMAT","AMCR","AMD","AME","AMG","AMGN","AMH","AMKR","AMN","AMP","AMPH","AMR",
"AMSF","AMT","AMTM","AMWD","AMZN","AN","ANDE","ANET","ANF","ANGI","ANIP","AON","AORT",
"AOS","AOSL","APA","APAM","APD","APG","APH","APLE","APLS","APO","APOG","APP","APPF",
"APTV","AR","ARCB","ARCT UQ","ARE","ARES","ARI","ARLO","ARM","ARMK","AROC","ARR","ARW","ARWR",
"AS","ASB","ASGN","ASH","ASIX","ASML","ASO","ASTE","ASTH","ASTS","ATAI","ATEN","ATGE",
"ATI","ATO","ATR","AU","AUB","AUR","AVA","AVAV","AVB","AVGO","AVNS","AVNT","AVT",
"AVTR","AVY","AWI","AWK","AWR","AX","AXL","AXON","AXP","AXS","AXTA","AYI","AZN",
"AZO","AZTA","AZZ","BA","BABA","BAC","BAH","BALL","BAM","BANC","BANF","BANR","BAX",
"BEAM","BEN","BEPC","BF.A","BF.B","BFAM","BFH","BFLY","BFS","BG","BGC","BHE","BHF",
"BIDU","BIIB","BILL","BIO","BIRK","BJ","BJRI","BK","BKE","BKH","BKNG","BKR","BKU",
"BL","BLD","BLDR","BLFS","BLK","BLKB","BLMN","BMI","BMRN","BMY","BOH","BOKF","BOOT",
"BOX","BPOP","BR","BRBR","BRC","BRK.B","BRKR","BRO","BROS","BRX","BSX","BSY","BTSG",
"BTU","BURL","BWA","BWXT","BX","BXMT","BXP","BYD","BYDDY","C","CABO","CACC","CACI",
"CADE","CAG","CAH","CAI","CAKE","CALM","CALX","CAR","CARG","CARR","CARS","CART","CASH",
"CASY","CAT","CATY","CAVA","CB","CBOE","CBRE","CBRL","CBSH","CBT","CBU","CC","CCCS",
"CCEP","CCI","CCJ","CCK","CCL","CCOI","CCS","CDNA","CDNS","CDP","CDW","CE","CEG",
"CELH","CENT","CENTA","CENX","CERS","CERT","CEVA","CF","CFFN","CFG","CFLT","CFR","CG",
"CGNX","CHCO","CHD","CHDN","CHE","CHEF","CHH","CHRD","CHRW","CHTR","CHWY","CI","CIEN",
"CINF","CIVI","CL","CLB","CLF","CLH","CLSK","CLVT","CLX","CMA","CMC","CMCSA","CME",
"CMG","CMI","CMPS","CMS","CNA","CNC","CNH","CNK","CNM","CNMD","CNO","CNP","CNR",
"CNS","CNX","CNXC","CNXN","COF","COHR","COHU","COIN","COKE","COLB","COLD","COLL","COLM",
"CON","COO","COOP","COP","COR","CORT","COST","COTY","CPAY","CPB","CPF","CPK","CPNG",
"CPRI","CPRT","CPRX","CPT","CR","CRC","CRGY","CRH","CRI","CRK","CRL","CRM","CROX",
"CRS","CRSP","CRSR","CRUS","CRVL","CRWD","CSCO","CSGP","CSGS","CSL","CSR","CSW","CSX",
"CTAS","CTKB","CTRA","CTRE","CTS","CTSH","CTVA","CUBE","CUBI","CUZ","CVBF","CVCO","CVI",
"CVLT","CVNA","CVS","CVX","CW","CWEN","CWEN.A","CWK","CWT","CXM","CXT","CXW","CYTK",
"CZR","D","DAL","DAN","DAR","DASH","DAY","DBX","DCI","DCOM","DD","DDOG","DDS",
"DE","DEA","DECK","DEI","DELL","DFH","DFIN","DG","DGII","DGX","DHI","DHR","DINO",
"DIOD","DIS","DJT","DKNG","DKNG UW","DKS","DLB","DLR","DLTR","DLX","DNB","DNOW","DOC",
"DOCN","DOCS","DOCU","DORM","DOV","DOW","DOX","DPZ","DRH","DRI","DRS","DT","DTE",
"DTM","DUK","DUOL","DV","DVA","DVAX","DVN","DXC","DXCM","DXPE","DY","EA","EAT",
"EBAY","ECG","ECL","ECPG","ED","EEFT","EFC","EFX","EG","EGBN","EGP","EHC","EIG",
"EIX","EL","ELAN","ELF","ELME","ELS","ELV","EMBC","EME","EMN","EMR","ENOV","ENPH",
"ENR","ENS","ENSG","ENTG","ENVA","EOG","EPAC","EPAM","EPC","EPR","EPRT","EQH","EQIX",
"EQR","EQT","ERIE","ES","ESAB","ESE","ESI","ESLT","ESNT","ESS","ESTC","ETD","ETN",
"ETOR","ETR","ETSY","EVR","EVRG","EVTC","EW","EWBC","EXAS","EXC","EXE","EXEL","EXLS",
"EXP","EXPD","EXPE","EXPI","EXPO","EXR","EXTR","EYE","EZPW","F","FAF","FANG","FAST",
"FBIN","FBK","FBNC","FBP","FBRT","FCF","FCFS","FCN","FCNCA","FCPT","FCX","FDP","FDS",
"FDX","FE","FELE","FERG","FFBC","FFIN","FFIV","FHB","FHI","FHN","FICO","FIS","FISV",
"FITB","FIVE","FIX","FIZZ","FLEX","FLG","FLO","FLR","FLS","FLUT","FMC","FN","FNB",
"FND","FNF","FORM","FOUR","FOX","FOXA","FOXF","FR","FRHC","FRPT","FRT","FSLR","FSS",
"FTAI","FTDR","FTI","FTNT","FTRE","FTV","FUL","FULT","FUN","FUTU","FWONA","FWONK","FWRD",
"FYBR","G","GAP","GATX","GBCI","GBX","GD","GDDY","GDEN","GDYN","GE","GEF","GEHC",
"GEN","GENI","GEO","GES","GEV","GFF","GFS","GGG","GH","GHC","GIII","GILD","GIS","GKOS",
"GL","GLBE","GLOB","GLPI","GLW","GM","GME","GMED","GNL","GNRC","GNTX","GNW","GO",
"GOGO","GOLF","GOOG","GOOGL","GPC","GPI","GPK","GPN","GRBK","GRMN","GRMN UN","GS","GSHD",
"GT","GTES","GTLB","GTLS","GTM","GTY","GVA","GWRE","GWW","GXO","H","HAE","HAFC",
"HAL","HALO","HAS","HASI","HAYW","HBAN","HBI","HCA","HCC","HCI","HCSG","HD","HEI",
"HEI.A","HELE","HFWA","HGV","HHH","HI","HIG","HII","HIMS","HIW","HL","HLI","HLIT",
"HLNE","HLT","HLX","HMN","HNI","HO","HOG","HOLX","HOMB","HON","HOOD","HOPE","HP",
"HPE","HPQ","HQY","HR","HRB","HRL","HRMY","HSIC","HSII","HST","HSTM","HSY","HTH",
"HTLD","HTO","HTZ","HUBB","HUBG","HUBS","HUM","HUN","HWC","HWKN","HWM","HXL","HZO",
"IAC","IART","IBKR","IBM","IBOC","IBP","IBTA","ICE","ICHR","ICUI","IDA","IDCC","IDXX",
"IEX","IFF","IIIN","IIPR","ILMN","INCY","INDB","INFA","INGM","INGR","INN","INSM","INSP",
"INSW","INTC","INTU","INVA","INVH","INVX","IONS","IOSP","IOT","IP","IPAR","IPG","IPGP",
"IQV","IR","IRDM","IRM","IRT","ISRG","IT","ITGR","ITRI","ITT","ITW","IVZ","J",
"JAZZ","JBGS","JBHT","JBL","JBLU","JBSS","JBTM","JCI","JEF","JHG","JHX","JJSF","JKHY",
"JLL","JNJ","JOBY","JOE","JPM","JXN","K","KAI","KALU","KAR","KBH","KBR","KD",
"KDP","KEX","KEY","KEYS","KFY","KGS","KHC","KIM","KKR","KLAC","KLAR","KLIC","KMB",
"KMI","KMPR","KMT","KMX","KN","KNF","KNSL","KNTK","KNX","KO","KOP","KR","KRC",
"KREF","KRG","KRYS","KSPI","KSS","KTB","KTOS","KVUE","KW","KWR","L","LAB","LAD",
"LAMR","LAZ","LBRDA","LBRDK","LBRT","LBTYA","LBTYK","LCID","LCII","LDOS","LEA","LECO",
"LEG","LEN","LEN.B","LFUS","LGIH","LGND","LH","LHX","LII","LIN","LINE","LITE","LIVN",
"LKFN","LKQ","LLY","LLYVA","LLYVK","LMAT","LMT","LNC","LNG","LNN","LNT","LNTH","LOAR",
"LOPE","LOW","LPG","LPLA","LPX","LQDT","LRCX","LRN","LSCC","LSTR","LTC","LULU","LUMN",
"LUNR","LUV","LVS","LW","LXP","LYB","LYFT","LYV","LZB","M","MA","MAA","MAC",
"MAN","MANH","MAR","MARA","MAS","MASI","MASS","MAT","MATW","MATX","MBC","MC","MCD",
"MCHP","MCK","MCO","MCRI","MCW","MCY","MD","MDB","MDLZ","MDT","MDU","MEDP","MELI",
"MET","META","MGEE","MGM","MGPI","MGY","MHK","MHO","MIDD","MIR","MKC","MKL","MKSI",
"MKTX","MLI","MLKN","MLM","MMC","MMI","MMM","MMS","MMSI","MNRO","MNST","MO","MODG",
"MOG.A","MOH","MORN","MOS","MP","MPC","MPW","MPWR","MRCY","MRK","MRNA","MRP","MRTN",
"MRVL","MS","MSA","MSCI","MSEX","MSFT","MSGS","MSI","MSM","MSTR","MTB","MTCH","MTD",
"MTDR","MTG","MTH","MTN","MTRN","MTSI","MTUS","MTX","MTZ","MU","MUR","MUSA","MWA",
"MXL","MYGN","MYRG","NABL","NATL","NAVI","NBHC","NBIX","NBTB","NCLH","NCNO","NDAQ",
"NDSN","NE","NEE","NEM","NEO","NEOG","NET","NEU","NFG","NFLX","NGVT","NHC","NI",
"NIQ","NJR","NKE","NLY","NMIH","NNN","NOC","NOG","NOV","NOVT","NOW","NPK","NPO",
"NRG","NRIX","NSA","NSC","NSIT","NSP","NTAP","NTCT","NTLA","NTNX","NTRA","NTRS","NU",
"NU UN","NUE","NVDA","NVR","NVRI","NVST","NVT","NWBI","NWE","NWL","NWN","NWS","NWSA",
"NX","NXDR","NXPI","NXRT","NXST","NXT","NYT","O","OC","ODFL","OFG","OGE","OGN",
"OGS","OHI","OI","OII","OKE","OKLO","OKTA","OLED","OLLI","OLN","OMC","OMCL","OMF",
"ON","ONB","ONON","ONTO","OPCH","ORA","ORCL","ORI","ORLY","OSIS","OSK","OTIS","OTTR",
"OUT","OVV","OWL","OXM","OXY","OZK","PACB","PAG","PAHC","PANW","PARR","PATH","PATK",
"PAYC","PAYO","PAYX","PB","PBF","PBH","PBI","PCAR","PCG","PCH","PCOR","PCRX","PCTY",
"PD","PDD","PDFS","PEB","PECO","PEG","PEGA","PEN","PENG","PENN","PEP","PFBC","PFE",
"PFG","PFGC","PFS","PG","PGNY","PGR","PH","PHIN","PHM","PI","PII","PINC","PINS",
"PIPR","PJT","PK","PKG","PLAB","PLAY","PLD","PLMR","PLNT","PLTR","PLUS","PLXS","PM",
"PMT","PNC","PNFP","PNR","PNW","PODD","POOL","POR","POST","POWI","POWL","PPC","PPG",
"PPL","PR","PRA","PRAA","PRDO","PRG","PRGO","PRGS","PRI","PRK","PRKS","PRLB","PRME",
"PRU","PRVA","PSA","PSKY","PSMT","PSN","PSNL","PSTG","PSX","PTC","PTEN","PTGX","PVH",
"PWR","PYPL","PZZA","Q","QCOM","QDEL","QGEN","QLYS","QNST","QRVO","QS","QSI",
"QSR","QTWO","QXO","R","RAL","RAMP","RARE","RBA","RBC","RBLX","RBRK","RC","RCL",
"RCUS","RDDT","RDN","RDNT","REG","REGN","RES","REX","REXR","REYN","REZI","RF","RGA",
"RGEN","RGLD","RGR","RH","RHI","RHP","RITM","RIVN","RJF","RKLB","RKLB UQ","RKT","RL",
"RLI","RMBS","RMD","RNG","RNR","RNST","ROCK","ROG","ROIV","ROK","ROKU","ROL","ROP",
"ROST","RPM","RPRX","RRC","RRR","RRX","RS","RSG","RTX","RUN","RUSHA","RVMD","RVTY",
"RWT","RXO","RXRX","RYAN","RYN","S","SABR","SAFE","SAFT","SAH","SAIA","SAIC","SAIL",
"SAM","SANM","SATS","SBAC","SBCF","SBH","SBRA","SBSI","SBUX","SCCO","SCHL","SCHW","SCI",
"SCL","SCSC","SCVL","SDGR","SE","SEB","SEDG","SEE","SEIC","SEM","SF","SFBS","SFD",
"SFM","SFNC","SGI","SHAK","SHC","SHEN","SHO","SHOO","SHOP","SHW","SIG","SIGI","SIRI",
"SITC","SITE","SITM","SJM","SKT","SKY","SKYW","SLAB","SLB","SLG","SLGN","SLM","SLMT",
"SLVM","SM","SMCI","SMG","SMMT","SMP","SMPL","SMTC","SN","SNA","SNCY","SNDR","SNEX",
"SNOW","SNPS","SNV","SNX","SO","SOFI","SON","SONO","SPG","SPGI","SPNT","SPOT","SPR",
"SPSC","SPXC","SR","SRE","SRPT","SSB","SSD","SSNC","SSTK","ST","STAA","STAG","STBA",
"STC","STE","STEL","STEP","STLD","STRA","STRL","STT","STWD","STX","STZ","SUI","SUPN",
"SW","SWK","SWKS","SWX","SXC","SXI","SXT","SYF","SYK","SYM UQ","SYNA","SYY","T",
"TALO","TAP","TBBK","TCBI","TDC","TDG","TDS","TDW","TDY","TEAM","TECH","TEL","TEM",
"TER","TEX","TFC","TFIN","TFSL","TFX","TGNA","TGT","TGTX","THC","THG","THO","THRM",
"THRY","THS","TIGO","TILE","TJX","TKO","TKR","TLN","TMDX","TMHC","TMO","TMP","TMUS",
"TNC","TNDM","TNL","TOL","TOST","TPG","TPH","TPL","TPR","TR","TREX","TRGP","TRI",
"TRIP","TRMB","TRMK","TRN","TRNO","TROW","TRST","TRU","TRUP","TRV","TSCO","TSLA","TSM",
"TSN","TT","TTC","TTD","TTEK","TTMI","TTWO","TW","TWI","TWLO","TWO","TWST","TXG",
"TXN","TXNM","TXRH","TXT","TYL","U","UA","UAA","UAL","UBER","UBSI","UCB","UCTT",
"UDR","UE","UFCS","UFPI","UFPT","UGI","UHAL","UHAL.B","UHS","UHT","UI","ULTA","UMBF",
"UNF","UNFI","UNH","UNIT","UNM","UNP","UPBD","UPS","URBN","URI","USB","USFD","USPH",
"UTHR","UTL","UVV","UWMC","V","VAC","VAL","VC","VCEL","VCTR","VCYT","VECO","VEEV",
"VFC","VIAV","VICI","VICR","VIK","VIR","VIRT","VKTX","VLO","VLTO","VLY","VMC","VMI",
"VNO","VNOM","VNT","VOYA","VRE","VRRM","VRSK","VRSN","VRT","VRTS","VRTX","VSAT","VSCO",
"VSH","VST","VSTS","VTLE","VTOL","VTR","VTRS","VVV","VYX","VZ","W","WAB","WABC",
"WAFD","WAL","WAT","WAY","WBD","WBS","WCC","WD","WDAY","WDC","WDFC","WEC","WELL",
"WEN","WERN","WEX","WFC","WFRD","WGO","WGS","WH","WHD","WHR","WING","WKC","WLK",
"WLY","WM","WMB","WMG","WMS","WMT","WOR","WPC","WRB","WRLD","WS","WSC","WSFS",
"WSM","WSO","WSR","WST","WT","WTFC","WTM","WTRG","WTS","WTW","WU","WWD","WWW",
"WY","WYNN","X:ADAUSD","X:ATOMUSD","X:AVAXUSD","X:BCHUSD","X:BTCUSD","X:DOGEUSD","X:DOTUSD","X:ETCUSD","X:ETHUSD","X:LINKUSD","X:LTCUSD",
"X:MATICUSD","X:SHIBUSD","X:SOLUSD","X:UNIUSD","X:XLMUSD","XEL","XHR","XNCR","XOM","XP","XPEL","XPO","XRAY",
"XYL","YELP","YETI","YOU","YUM","Z","ZBH","ZBRA","ZD","ZG","ZION","ZM","ZS","ZTS","ZWS"]
        tickers = [t for t in tickers if t in TICKERS_FULL]

        logging.info("Filtered tickers (override): %s", tickers)

        # Load all existing ticker_date_ids once
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT ticker_date_id
                FROM {target_schema}.{target_table}
            """)
            existing_ids = {row[0] for row in cursor.fetchall()}
            logging.info("Existing IDs in target table: %d", len(existing_ids))

        # ---------------------------------------------------------
        # PROCESS IN BATCHES
        # ---------------------------------------------------------
        for i in range(0, len(tickers), batch_size):
            ticker_batch = tickers[i:i + batch_size]
            logging.info("Processing batch %d–%d: %s", i + 1, i + len(ticker_batch), ticker_batch)

            # Fetch raw data
            placeholders = ",".join(["%s"] * len(ticker_batch))
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT * FROM {schema_name}.{table_name}
                    WHERE ticker IN ({placeholders})
                    """,
                    ticker_batch,
                )
                raw_data = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]

            if not raw_data:
                logging.info("No data found for this batch.")
                continue

            # Clean and to Polars
            cleaned = [clean_row(row, colnames) for row in raw_data]

            schema = {
                "date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "dividends": pl.Float64,
                "stock_splits": pl.Float64,
                "ticker": pl.Utf8,
                "processed_at": pl.Utf8,
                "adj_close": pl.Float64,
                "capital_gains": pl.Utf8,
                "ticker_date_id": pl.Utf8,
            }

            pl_df = pl.DataFrame(cleaned, schema=schema)

            # Fill missing dates
            full_df = generate_full_date_range(pl_df)
            if full_df.is_empty():
                logging.info("No data after date expansion for this batch.")
                del pl_df
                gc.collect()
                continue

            # Smart rounding + metadata
            full_df = full_df.with_columns(
                [
                    pl.when(pl.col("adj_close").is_null())
                    .then(None)
                    .when(pl.col("adj_close") < 10)
                    .then(pl.col("adj_close").round(3))
                    .otherwise(pl.col("adj_close").round(2))
                    .alias("adj_close"),

                    (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
                    pl.lit(PROCESSED_AT_PLUS_2H.isoformat()).alias("processed_at"),
                    pl.lit("yfinance").alias("source"),
                ]
            )

            # Sort chronologically
            full_df = full_df.sort(["ticker", "date"])

            # Backward fill missing values for synthetic rows
            cols_to_bfill = [
                "open", "high", "low", "close", "volume",
                "dividends", "stock_splits", "adj_close", "capital_gains",
            ]

            next_vals = {
                c: (
                    pl.when(pl.col("date_type") == "natural")
                    .then(pl.col(c))
                    .otherwise(None)
                    .backward_fill()
                    .over("ticker")
                )
                for c in cols_to_bfill
            }

            full_df = full_df.with_columns(
                [
                    pl.when((pl.col("date_type") == "synthetic") & pl.col(c).is_null())
                    .then(next_vals[c])
                    .otherwise(pl.col(c))
                    .alias(c)
                    for c in cols_to_bfill
                ]
            )

            full_df = full_df.with_columns(pl.col("volume").cast(pl.Int64, strict=False))

            # Final cleanup + dedupe
            full_df = (
                full_df.select(
                    [
                        "date", "ticker", "open", "high", "low", "close", "volume",
                        "dividends", "stock_splits", "processed_at", "adj_close",
                        "capital_gains", "date_type", "ticker_date_id", "source"
                    ]
                )
                .unique(subset=["ticker_date_id"])
            )

            # Remove existing IDs
            full_df = full_df.filter(~pl.col("ticker_date_id").is_in(existing_ids))
            if full_df.is_empty():
                logging.info("No new rows to insert for batch.")
                del pl_df, full_df
                gc.collect()
                continue

            logging.info("New rows to insert: %d", full_df.shape[0])

            # Insert in chunks
            for start in range(0, full_df.shape[0], insert_batch_size):
                end = min(start + insert_batch_size, full_df.shape[0])
                insert_batch = full_df[start:end]

                csv_buffer = io.StringIO()
                insert_batch.write_csv(csv_buffer)
                csv_buffer.seek(0)

                try:
                    with conn.cursor() as cursor:
                        cursor.copy_expert(
                            f"""COPY {target_schema}.{target_table} (
                                "date", ticker, "open", high, low, "close", volume,
                                dividends, "stock_splits", processed_at, adj_close,
                                "capital_gains", date_type, ticker_date_id, source
                            ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)""",
                            csv_buffer,
                        )
                    conn.commit()
                    logging.info("Inserted %d rows.", insert_batch.shape[0])
                except Exception as e:
                    logging.error("Failed to insert rows %d–%d: %s", start, end, e)

            del pl_df, full_df
            gc.collect()

except psycopg2.Error as db_err:
    logging.error("Database error: %s", db_err)
except Exception as e:
    logging.error("Unexpected error: %s", e)