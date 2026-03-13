import pandas as pd
import glob
import os
import re
import concurrent.futures
import itertools
from urllib.parse import urlparse, parse_qs
import igraph as ig
import leidenalg as la
import gc
from collections import Counter, defaultdict
import math
import threading
import holoviews as hv
from holoviews import opts

# ==============================================================================
# SCIENTIFIC METHODOLOGY & DISCLAIMER
# ==============================================================================

def print_scientific_disclaimer():
    print("=" * 80)
    print("KNIGHTFALL ENGINE - SCIENTIFIC METHODOLOGY & LIMITATIONS NOTE")
    print("=" * 80)
    print("""
    [CRITICAL ADVISORY]
    MANUAL REVIEW OF EVERY OUTPUT IS HIGHLY ADVISABLE BEFORE USING THE DATA 
    FOR ANY REPORTING OR RESEARCH PURPOSES.

    This engine uses probabilistic and statistical modeling to infer behavior. 
    While robust, it operates on "Signatures of Coordinated Behavior", which can 
    occasionally overlap with organic high-intensity activism.

    LOGICAL GAPS & COUNTER-MEASURES:
    
    1. GAP: Coincidence vs. Coordination (The "Viral Problem")
       - ADMISSION: In highly viral events, thousands of organic users attack 
         the same target. High overlap does not always grant conspiracy.
       - COUNTER: We implement 'Inverse Frequency Weighting' (1/sqrt(N)). 
         Links formed on viral posts are mathematically suppressed. 
         Only repeated, strictly patterned overlaps across *multiple* unrelated 
         events trigger a strong "Coordination" signal.

    2. GAP: Dynamic Threshold Bias (The "Top 5% Problem")
       - ADMISSION: The engine dynamically flags the top 5% of hostile actors. 
         In a completely benign dataset, the "worst" 5% might still be harmless.
       - COUNTER: These flags are relative, not absolute. They identify the 
         *comparative* outliers within the specific context of your investigation.
         Analyst discretion is required to determine if the "Elite" threshold 
         (e.g., Hostility Score 2.5 vs 15.0) represents genuine threats.

    3. GAP: Forensic Script Detection
       - ADMISSION: Regex detection of specific scripts (e.g., Cyrillic, Burmese)
         is not 100% proof of geographic origin. Diaspora populations or 
         aesthetic font choices (Math Sans) can trigger these.
       - COUNTER: The engine uses a "Priority Hierarchy" to filter common false 
         positives, but all "Foreign Script" flags should be treated as 
         leads for investigation, not proof of bot farm origin.

    4. GAP: Tactical Triangulation
       - ADMISSION: Co-reacting differs from Co-conspiring.
       - COUNTER: The graph construction relies on 'Modularity Optimization' 
         (Leiden Algorithm). It ignores weak, random connections and only 
         solidifies clusters that are mathematically denser than random chance.
    """)
    print("=" * 80 + "\n")

# ==============================================================================
# DYNAMIC CONFIGURATION ARCHITECTURE
# ==============================================================================

class KnightfallConfig:
    """
    Central configuration controller for the Knightfall Engine.
    
    Design Philosophy:
    This engine is designed to be dataset-agnostic. Instead of relying on hardcoded
    thresholds (which may vary by region or campaign intensity), it employs
    Dynamic Statistical Thresholding. Limits for 'Elite' actors or 'High Frequency'
    attacks are calculated at runtime based on the 90th/95th percentiles of the
    actual input data.
    """
    # --- Adaptive Thresholds (Calculated at Runtime) ---
    # Set to None to trigger auto-calibration in 'calibrate_dynamic_thresholds()'
    MIN_COORDINATION_WEIGHT = None  # Auto-calculated: 90th Percentile of link weights in the graph
    THRESHOLD_ELITE_HOSTILITY = None  # Auto-calculated: Top 5% (95th Percentile) of all hostility scores
    THRESHOLD_HIGH_FREQ_ATTACKS = None # Auto-calculated: Top 5% of user activity volume
    
    # --- Hard Limits & Performance Tuning ---
    MIN_CLUSTER_SIZE = 10 # Minimum nodes required to constitute a valid 'Tactical Unit'
    POST_CAP_SIZE = 10000 # Performance Cap: Max users processed per post to prevent OOM on viral posts.
    
    # --- Scoring Weights ---
    # These base weights define the severity of different interaction types.
    # used to calculate the cumulative 'Hostility Score'.
    WEIGHTS = {
        'ID': 1.0,    # Baseline Support (Likes/Comments positive)
        'TGT': 2.0,   # Direct Targeted Harassment/Rivalry
        'BTL': 4.0    # Battleground/Media Manipulation (Highest Hostility)
    }

# ==============================================================================
# FORENSIC SCRIPT DETECTION ENGINE
# ==============================================================================
# This module identifies the linguistic origin of user profiles based on 
# Unicode block ranges. It is used to detect potential "Click Farm" activity
# originating from non-native regions (e.g., SE Asia, Russia, Eastern Europe).
#
# NOTE: Manual analyst overview is required. Regex is probabilistic.
# legitimate users may use foreign scripts (e.g. diaspora populations).

REGEX_FOREIGN = {
    # Tier 1: Evasion & Obfuscation (Mathematical Alphanumerics, often used to bypass filters)
    'Obfuscated (Math/Fancy)': re.compile(r'[\U0001D400-\U0001D7FF]'),

    # Tier 2: High-Volume Bot Hubs (Primary commercial click-farm sources)
    'Myanmar (Burmese)': re.compile(r'[\u1000-\u109F]'),
    'Russian (Cyrillic)': re.compile(r'[\u0400-\u04FF\u0500-\u052F]'),
    'Chinese (Hanzi)': re.compile(r'[\u4E00-\u9FFF]'),
    'Thai': re.compile(r'[\u0E00-\u0E7F]'),
    'Lao': re.compile(r'[\u0E80-\u0EFF]'),
    'Cambodian (Khmer)': re.compile(r'[\u1780-\u17FF]'),
    
    # Vietnamese Detection (Aggressive coverage of tonal marks common in commercial accounts)
    'Vietnamese': re.compile(r'[\u1EA0-\u1EF9\u1E00-\u1EFF\u01A0-\u01B0\u0110-\u0111\u00CA\u00EA\u00D4\u00F4\u0300-\u036F\u00E2\u00C2\u0103\u0102\u0168-\u0169\u0128-\u0129\u00FD\u00DD]'),

    # Tier 3: Regional Indicators (Context-dependent relevance)
    'Portuguese (Angola/Moz/Brazil)': re.compile(r'[ãõÃÕçÇ]'),
    'Filipino/Spanish': re.compile(r'[ñÑ]'),
    'French (West Africa)': re.compile(r'[èëïÈËÏ]'),

    # Tier 4: South Asian & Others
    'Hindi (Devanagari)': re.compile(r'[\u0900-\u097F]'),
    'South India (Dravidian)': re.compile(r'[\u0B80-\u0D7F]'),
    'Korean (Hangul)': re.compile(r'[\uAC00-\uD7AF\u1100-\u11FF]'),
    'Japanese': re.compile(r'[\u3040-\u30C2\u30C6-\u30FA]'),
    'Philippines (Tagalog Script)': re.compile(r'[\u1700-\u171F]'),
    
    # Tier 5: Global/Other
    'Greek': re.compile(r'[\u0370-\u03FF]'),
    'Ethiopic (Amharic)': re.compile(r'[\u1200-\u137F]'),
    
    # Fallback / Ambiguous
    'Phonetic / IPA Script': re.compile(r'[\u0250-\u02AF\u1D00-\u1D7F\u02B0-\u02FF]'),
    'Latin Extended (Unidentified)': re.compile(r'[\u00C0-\u00FF\u0100-\u017F]')
}

def get_priority_flag(flag_set):
    """
    Determines the primary forensic region based on a priority hierarchy.
    Crucial for avoiding false positives (e.g., classifying a Spanish name as Filipino).
    Prioritizes known high-risk bot-farm locations.
    """
    if not flag_set: return None
        
    hierarchy = [
        # Priority 1: High-Risk Hubs (Highest probability of inorganic activity)
        'Invisible (Zero-Width)', 'Obfuscated (Math/Fancy)',
        'Myanmar (Burmese)', 'Vietnamese', 'Thai', 'Lao', 'Cambodian (Khmer)',
        'Russian (Cyrillic)', 'Chinese (Hanzi)',
        
        # Priority 2: Distinctive Latin variants
        'Filipino/Spanish', 'Portuguese (Angola/Moz/Brazil)', 'French (West Africa)',
        
        # Priority 3: Regional
        'Hindi (Devanagari)', 'South India (Dravidian)', 'Korean (Hangul)', 
        'Japanese', 'Philippines (Tagalog Script)',
        
        # Priority 4: Other
        'Arabic/Persian', 'Greek', 'Hebrew', 'Ethiopic (Amharic)',
        
        # Priority 5: Ambiguous
        'Phonetic / IPA Script', 'Latin Extended (Unidentified)' 
    ]
    
    for h in hierarchy:
        if h in flag_set:
            return h
            
    return sorted(list(flag_set))[0]

def detect_foreign_script(text):
    """
    Analyzes a string (username) and returns the detected script/region category.
    Returns: String (Category) or None (if Latin/Safe).
    """
    if not isinstance(text, str):
        return None
        
    detected_flags = set()
    for label, pattern in REGEX_FOREIGN.items():
        if pattern.search(text):
            detected_flags.add(label)
            
    if not detected_flags:
        return None 
        
    return get_priority_flag(detected_flags)


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def normalize_fb_url(url):
    """
    Standardizes Facebook profile URLs to ensure consistent deduplication.
    Handles mobile prefixes (m.facebook, web.facebook) and extracts numerical IDs 
    from profile.php structure to ensure 'fb.com/user1' and 'm.fb.com/user1' match.
    """
    if not isinstance(url, str): return "Unknown"
    url = url.lower().strip()
    url = url.replace('m.facebook.com', 'facebook.com').replace('web.facebook.com', 'facebook.com')
    parsed = urlparse(url)
    if "profile.php" in parsed.path:
        qs = parse_qs(parsed.query)
        user_id = qs.get('id', [None])[0]
        if user_id: return f"facebook.com/profile.php?id={user_id}"
    path = parsed.path.strip('/')
    if not path: return url
    return f"facebook.com/{path}"

def process_single_file(filepath):
    """
    Worker function to process a single CSV file in parallel.
    Extracts name, url, and metadata.
    
    Filename Convention Required: TYPE_PARTY_PAGE_POST.csv
    - TYPE: ID, TGT, BTL (Interaction Type)
    - PARTY: AL, BNP, JAM, etc. (Target/Source Affiliation)
    - PAGE: Page Name
    - POST: Post ID/Descriptor
    """
    filename = os.path.basename(filepath)
    clean_name = filename.replace(".csv", "")
    parts = clean_name.split("_", 3)
    if len(parts) < 4: return {'error': f"Malformed Filename: {filename}"}
    
    f_type, f_party, f_page, f_post = parts[0], parts[1], parts[2], parts[3]
    event_fingerprint = f"{f_page}_{f_post}" 
    
    results = []
    try:
        # Memory Optimization: Only load columns containing 'url' or 'name'
        # This reduces memory footprint by ~60% on large datasets.
        df = pd.read_csv(filepath, usecols=lambda c: 'url' in c.lower() or 'name' in c.lower(),
                         encoding='utf-8-sig', on_bad_lines='skip', low_memory=False)
        if df.empty: return {'data': []}
        
        name_col = next((c for c in df.columns if 'name' in c.lower()), None)
        url_col = next((c for c in df.columns if 'url' in c.lower()), None)
        if not name_col or not url_col: return {'error': f"Missing columns in {filename}"}
        
        df[name_col] = df[name_col].astype('category')
        df[url_col] = df[url_col].astype('category')

        for raw_name, raw_url in zip(df[name_col], df[url_col]):
            clean_url = normalize_fb_url(str(raw_url))
            results.append({
                'name': str(raw_name).strip(),
                'url': clean_url,
                'type': f_type,
                'party': f_party,
                'page': f_page,
                'event_id': event_fingerprint 
            })
    except Exception as e: return {'error': f"Read Error {filename}: {str(e)}"}
    return {'data': results}


# ==============================================================================
# CORE ENGINE: KNIGHTFALL ARCHITECT
# ==============================================================================

class KnightfallArchitect:
    """
    The central intelligence engine.
    Orchestrates Data Ingestion -> Forensic Profiling -> Network Construction -> Clustering -> Reporting.
    """
    def __init__(self):
        self.user_db = {}
        self.all_files = glob.glob("*.csv")
        self.file_participants = defaultdict(set) # Tracks users per file for Master Stats
        self.lock = threading.Lock()
        
        # Load Known Bot Profiles for Cross-Referencing
        # These are pre-identified commercial/political bot lists (if available)
        self.known_bots = set()
        print("[*] Loading 'Bot Profiles' database...")
        try:
            bot_files = glob.glob("Bot Profiles/*.csv")
            for bf in bot_files:
                try:
                    b_df = pd.read_csv(bf)
                    clean_urls = b_df['Profile URL'].astype(str).apply(lambda x: normalize_fb_url(x))
                    self.known_bots.update(clean_urls.tolist())
                except Exception as e:
                    print(f"[!] Error reading bot file {bf}: {e}")
            print(f"[*] Loaded {len(self.known_bots)} confirmed bot profiles.")
        except Exception as e:
            print(f"[!] Critical Error loading Bot Profiles: {e}")

        # Initialize Thresholds (Will be calibrated based on data distribution after ingestion)
        self.elite_threshold = KnightfallConfig.THRESHOLD_ELITE_HOSTILITY
        self.high_freq_threshold = KnightfallConfig.THRESHOLD_HIGH_FREQ_ATTACKS

    def calibrate_dynamic_thresholds(self):
        """
        [DYNAMIC CONFIGURATION]
        This function analyzes the statistical distribution of the ingested data
        to automatically set threshold limits. This replaces hardcoded constants,
        allowing the engine to adapt to small vs large datasets without manual tuning.
        """
        print("[*] Calibrating Dynamic Thresholds based on Dataset Statistics...")
        
        # 1. Hostility Threshold (Elite Actors)
        # We define 'Elite' actors as those in the top 5% of hostility scores.
        if self.elite_threshold is None:
            hostility_scores = [d['hostility_score'] for d in self.user_db.values() if d['hostility_score'] > 0]
            if hostility_scores:
                self.elite_threshold = pd.Series(hostility_scores).quantile(0.95)
                print(f"    -> Dynamic Elite Hostility Threshold set to: {self.elite_threshold:.2f} (Top 5%)")
            else:
                self.elite_threshold = 15.0 # Fallback default
                print(f"    -> Fallback Elite Hostility Threshold set to: {self.elite_threshold:.2f}")

        # 2. High Frequency Threshold (Event Count)
        # We identify 'High Frequency' attackers as those in the top 5% of activity volume.
        if self.high_freq_threshold is None:
            attack_counts = [len(d['attack_events']) for d in self.user_db.values() if len(d['attack_events']) > 0]
            if attack_counts:
                self.high_freq_threshold = pd.Series(attack_counts).quantile(0.95)
                print(f"    -> Dynamic High Frequency Threshold set to: {self.high_freq_threshold:.1f} events (Top 5%)")
            else:
                self.high_freq_threshold = 15 # Fallback default
                print(f"    -> Fallback High Frequency Threshold set to: {self.high_freq_threshold}")

    def run_parallel_ingestion(self):
        """
        Ingests all CSV files in the directory using parallel processing (ProcessPoolExecutor).
        Aggregates user data, event participation, and calculates initial hostility scores.
        """
        print(f"[*] Starting Multiprocess Ingestion for {len(self.all_files)} files...")
        with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
            future_to_file = {executor.submit(process_single_file, f): f for f in self.all_files}
            for future in concurrent.futures.as_completed(future_to_file):
                filepath = future_to_file[future]
                filename = os.path.basename(filepath)
                
                result = future.result()
                if 'error' in result: continue
                
                for record in result['data']:
                    url = record['url']
                    
                    # Track interactions for file-level statistics
                    if record['type'] in ['TGT', 'BTL']:
                        self.file_participants[filename].add(url)
                    
                    # Initialize user record if new
                    if url not in self.user_db:
                        self.user_db[url] = {
                            'name': record['name'],
                            'affiliation_tally': Counter(),
                            'hostility_score': 0.0,
                            'attack_events': set(), 
                            'support_events': set(),
                            'attacked_parties': Counter(),
                            'targeted_entities': Counter(),
                            'evidence_log': {}
                        }
                    
                    # Update name if previously missing/malformed
                    curr = self.user_db[url].get('name', '')
                    if record['name'] and (not curr or curr == "nan"):
                        self.user_db[url]['name'] = record['name']
                    
                    user = self.user_db[url]
                    
                    # Process Interaction based on Type (ID=Support, TGT/BTL=Attack)
                    if record['type'] == 'ID':
                        user['support_events'].add(record['event_id'])
                        user['affiliation_tally'][record['party']] += 1
                    elif record['type'] in ['TGT', 'BTL']:
                        user['attack_events'].add(record['event_id'])

                        category = record['party']
                        target_page = record['page']

                        # Track Media Targeting specifically (Attacks on Journalism)
                        if category in ['MEDIA', 'JOURN']:
                            if 'targeted_media' not in user:
                                user['targeted_media'] = Counter()
                            user['targeted_media'][target_page] += 1
                        
                        # Apply Hostility Weights
                        # Attacks on Press (JOURN) calculate at max hostility (4.0)
                        if record['party'] == 'JOURN' or record['type'] == 'BTL':
                            user['hostility_score'] += 4.0
                        else:
                            user['hostility_score'] += KnightfallConfig.WEIGHTS.get(record['type'], 1.0)
                            
                        user['attacked_parties'][record['party']] += 1
                        user['targeted_entities'][record['page']] += 1
                        user['evidence_log'][record['event_id']] = {
                            'target': record['party'], 
                            'page': record['page'], 
                            'type': record['type']
                        }
        
        # After ingestion is complete, calibrate thresholds based on data distribution
        self.calibrate_dynamic_thresholds()

    def resolve_affiliations(self):
        """
        [FORENSIC ALIGNMENT ALGORITHM]
        Determines the political alignment of users even if they never explicitly state it.
        
        Methodology:
        1. Fingerprinting: Analyze users with explicit 'ID' (Support) interactions to build
           a 'Targeting Fingerprint' for each political faction.
           (e.g., "A known party's supporters typically attack Page X, Y, and Z").
           
        2. Tactical Alignment: Match unknown users ('Independent Attackers') to these fingerprints.
           If an unknown user's attack pattern matches the party Fingerprint (sharing 3+ targets),
           they are labeled "Tactically Aligned: 'Party Name'". However we are keeping it separate as not to pollute the pure political squads.
        """
        print("[*] Performing Forensic Behavioral Profiling...")
        
        # Pass 1: Build Fingerprints from ID-Verified users
        party_fingerprints = {}
        for url, data in self.user_db.items():
            if data['affiliation_tally']:
                party = data['affiliation_tally'].most_common(1)[0][0]
                if party not in party_fingerprints: party_fingerprints[party] = Counter()
                party_fingerprints[party].update(data['targeted_entities'])

        # Refine Fingerprint: Keep only top 10 most characteristic targets per faction
        for p in party_fingerprints:
            party_fingerprints[p] = set([t for t, _ in party_fingerprints[p].most_common(10)])

        # Pass 2: Assign Identities and Alignment
        for url, data in self.user_db.items():
            if data['affiliation_tally']:
                data['primary_affiliation'] = data['affiliation_tally'].most_common(1)[0][0]
                data['behavioral_alignment'] = "Verified Core"
            elif data['attacked_parties']:
                top_target = data['attacked_parties'].most_common(1)[0][0]
                data['primary_affiliation'] = f"ANTI_{top_target.upper()}"

                user_targets = set(data['targeted_entities'].keys())
                for p_name, p_fingerprint in party_fingerprints.items():
                    # Logic Check: You cannot align with a party you are attacking. Keeping alignment purity and ignoring noise.
                    if f"ANTI_{p_name.upper()}" in data['primary_affiliation']:
                        continue
                        
                    # Requirement: User must share at least 3 identical targets with the faction
                    if len(user_targets.intersection(p_fingerprint)) >= 3: 
                        data['behavioral_alignment'] = f"Tactically Aligned: {p_name}"
                        break
                else:
                    data['behavioral_alignment'] = "Independent Attacker"
            else:
                data['primary_affiliation'] = "Neutral/Unknown"
                data['behavioral_alignment'] = "None"
            data['is_id_verified'] = len(data['affiliation_tally']) > 0

    def detect_coordinated_clusters(self):
        """
        [LEIDEN CLUSTERING & TACTICAL TRIANGULATION]
        
        This method constructs the network graph of users and detects coordinated squads.
        
        Steps:
        1. Tactical Triangulation: Users are linked if they attack the SAME post/event.
           (Hypergraph projection -> User-User Graph).
        2. Weighting Strategy:
           - Links are penalized for viral posts (1 / sqrt(N)). Co-reacting on a viral post is weak evidence.
           - Links are penalized for high-activity users (logarithmic scaling).
        3. Thresholding: Only edges with weight > MIN_COORDINATION_WEIGHT are kept.
        4. Leiden Algorithm: Optimizes modularity to find dense clusters (Squads).
        """
        print("[*] Initiating Weighted Tactical Triangulation...")
        url_to_id = {url: i for i, url in enumerate(sorted(self.user_db.keys()))}
        id_to_url = {i: url for url, i in url_to_id.items()}
        
        # 1. Pre-calculate Target Rarity (Inverse Frequency Weighting)
        full_inverted_index = defaultdict(list)
        for uid, url in id_to_url.items():
            for event_id in self.user_db[url]['attack_events']:
                full_inverted_index[event_id].append(uid)

        # Weighting: 1 / sqrt(N). Reduces impact of viral mass-events heavily.
        target_weights = {eid: 1.0 / (len(uids)**0.5) for eid, uids in full_inverted_index.items()}

        # 2. Activity Filter: Process only active participants (>= 7 events) to remove noise
        active_ids = {uid for uid, url in id_to_url.items() if len(self.user_db[url]['attack_events']) >= 7}
        pair_weights = Counter()

        def get_clean_party(a):
            return str(a).upper().strip() if a else "NEUTRAL"

        self.bridge_matrix = Counter()
        pair_shared_targets = defaultdict(set) 
        
        # 3. Weighted Linking Loop
        user_activity = {uid: len(self.user_db[id_to_url[uid]]['attack_events']) for uid in active_ids}
        
        for event_id, users in full_inverted_index.items():
            if len(users) < 2: continue
            
            # Performance cap for viral posts
            if len(users) > KnightfallConfig.POST_CAP_SIZE:
                users = sorted(users, key=lambda u: self.user_db[id_to_url[u]]['hostility_score'], reverse=True)[:KnightfallConfig.POST_CAP_SIZE] 
            
            current_event_weight = target_weights[event_id]
            active_in_event = [u for u in users if u in active_ids]
            
            # Create links between all co-attackers in this event
            for u1, u2 in itertools.combinations(sorted(active_in_event), 2):
                # Logarithmic Scaling: Penalizes super-active users who naturally overlap by chance
                act1 = user_activity[u1]
                act2 = user_activity[u2]
                normalized_weight = current_event_weight / (math.log(act1 + 2) * math.log(act2 + 2))
                
                data1, data2 = self.user_db[id_to_url[u1]], self.user_db[id_to_url[u2]]
                p1, p2 = get_clean_party(data1['primary_affiliation']), get_clean_party(data2['primary_affiliation'])

                # CRITICAL: Segregation Logic
                # If two users belong to OPPOSING factions, they are NOT linked as a squad.
                # Instead, we log this as a "Bridge" (Cross-Faction Interaction) for the Chord Diagram.
                if p1 != p2:
                    bridge_key = "_<->_".join(sorted([p1, p2]))
                    self.bridge_matrix[bridge_key] += 1
                    continue 

                pair_weights[(u1, u2)] += normalized_weight
                t1 = data1['evidence_log'].get(event_id, {}).get('page')
                if t1:
                    pair_shared_targets[(u1, u2)].add(t1)

        if pair_weights:
            max_w = max(pair_weights.values())
            print(f"[*] Intelligence Insight: Strongest coordination link found is {max_w:.4f}")
        
        # 4. Filter edges by threshold (Dynamic or Static)
        if KnightfallConfig.MIN_COORDINATION_WEIGHT is None:
            all_weights = list(pair_weights.values())
            if all_weights:
                # 90th Percentile: Only the top 10% strongest links are preserved.
                COORD_THRESHOLD = pd.Series(all_weights).quantile(0.90)
                print(f"[*] Dynamic Thresholding: Auto-set Coordination Weight to {COORD_THRESHOLD:.4f} (90th Percentile)")
            else:
                COORD_THRESHOLD = 0.015 # Safety Fallback
        else:
            COORD_THRESHOLD = KnightfallConfig.MIN_COORDINATION_WEIGHT

        edges = [(u1, u2) for (u1, u2), w in pair_weights.items() if w >= COORD_THRESHOLD]
        weights = [w for (u1, u2), w in pair_weights.items() if w >= COORD_THRESHOLD]
        
        del pair_weights, full_inverted_index
        gc.collect()

        if not edges: 
            print("[!] No coordinated units detected above tactical threshold.")
            return

        # 5. Graph Construction (igraph)
        print("[*] Constructing Tactical Graph Architecture...")
        self.ig_graph = ig.Graph(len(self.user_db))
        self.ig_graph.add_edges(edges)
        self.ig_graph.es['weight'] = weights

        # 6. Leiden Clustering (Community Detection)
        # Using Modularity Vertex Partition to find natural groups
        partition = la.find_partition(self.ig_graph, la.ModularityVertexPartition, weights='weight', seed=42)
        degrees = self.ig_graph.degree()

        cluster_id = 1
        for cluster_indices in partition:
            if len(cluster_indices) >= KnightfallConfig.MIN_CLUSTER_SIZE:
                all_affs = [self.user_db[id_to_url[uid]]['primary_affiliation'] for uid in cluster_indices]
                total_size = len(cluster_indices)
                
                # Naming Logic:
                # Label cluster based on the dominant political affiliation of its members.
                hard_identities = [a for a in all_affs if "Neutral" not in a and "Suspected" not in a]
                soft_identities = [a for a in all_affs if "Suspected" in a]
                
                identified_count = len(hard_identities) + len(soft_identities)
                density = identified_count / total_size

                if density < 0.20:
                    label = f"ANONYMOUS_TACTICAL_UNIT_{cluster_id}"
                else:
                    pool = hard_identities if hard_identities else soft_identities
                    top_party, _ = Counter(pool).most_common(1)[0]
                    clean_name = top_party.replace("Suspected_", "").replace("_Rival", "")
                    
                    if "ANTI_" in top_party:
                        label = f"{clean_name}_TARGETING_UNIT_{cluster_id}"
                    else:
                        label = f"{clean_name}_STRIKE_SQUAD_{cluster_id}"

                urls = [id_to_url[uid] for uid in cluster_indices]
                avg_hostility = sum(self.user_db[u]['hostility_score'] for u in urls) / len(urls)
                
                # Elite Designator: If avg hostility > Threshold, mark as ELITE.
                if avg_hostility >= self.elite_threshold: 
                    label = "ELITE_" + label

                print(f" [!] {label}: {len(urls)} Members (Avg Hostility: {avg_hostility:.1f})")
                for url in urls: self.user_db[url]['cluster_membership'] = label
                cluster_id += 1
            else:
                for uid in cluster_indices:
                    url = id_to_url[uid]
                    self.user_db[url]['cluster_membership'] = "Small_Cell" if degrees[uid] > 0 else "Individual"

    def generate_chord_diagram(self):
        """
        [VISUALIZATION]
        Generates a customized Chord Diagram using HoloViews/Bokeh.
        This visualizes the 'Coordinated Bridges' (Cross-Faction links) detected
        during the triangulation phase. 
        """
        try:
            hv.extension('bokeh')
            hv.renderer('bokeh').theme = 'dark_minimal'
            
            # Aggregate connections
            chord_data = [] 
            
            # Party Label Mapping (Can be customized for localization)
            party_map = {
                "AL": "AWAMI LEAGUE (AL)",
                "BNP": "BNP (Nationalist)",
                "JAM": "JAMAAT (JAM)",
                "NCP": "NCP (Citizen)"
                # Add more mappings as needed
            }
            
            for edge_key, weight in self.bridge_matrix.items():
                parts = edge_key.split("_<->_")
                if len(parts) == 2:
                    if weight > 5: # Filter noise
                        s_name = party_map.get(parts[0], parts[0])
                        t_name = party_map.get(parts[1], parts[1])
                        chord_data.append((s_name, t_name, weight))
            
            if chord_data:
                chord_df = pd.DataFrame(chord_data, columns=['source', 'target', 'value'])
                chord = hv.Chord(chord_df)
                
                # Visual Options (Cyan/Purple Palette)
                chord.opts(
                    opts.Chord(
                        cmap=['#7B00FF', '#00F2FF', '#FF0055', '#39FF14', '#FFAA00'],
                        edge_cmap='cool', 
                        edge_color='source', 
                        labels='index', 
                        node_color='index',
                        edge_alpha=0.6,
                        node_size=30, 
                        width=900, height=900, 
                        bgcolor='#0a0f1e',
                        title="CROSS-FACTION COORDINATION BRIDGES",
                        label_text_font_size='14pt',
                        label_text_color='#00d4ff',
                        label_text_font_style='bold' 
                    )
                )
                
                if not os.path.exists("Investigative_Archive"): os.makedirs("Investigative_Archive")
                output_path = 'Investigative_Archive/Strategic_Chord_Diagram.html'
                hv.save(chord, output_path, backend='bokeh')
                
                print("[✔] Chord Diagram generated in Investigative_Archive.")
        except Exception as e:
            print(f"[!] Chord Diagram Error: {e}")

    def generate_file_level_bot_report(self):
        """
        Scans raw TGT_ and BTL_ CSV files to count Total Profiles vs Confirmed Bots.
        """
        print("[*] Running File-Level Bot Analysis on Raw CSVs...")
        results = []
        target_files = [f for f in self.all_files if f.startswith("TGT_") or f.startswith("BTL_")]
        
        for filepath in target_files:
             try:
                # Optimized partial read to check header
                df = pd.read_csv(filepath, encoding='utf-8-sig', on_bad_lines='skip', low_memory=False, nrows=1)
                url_col = next((c for c in df.columns if 'url' in c.lower()), None)
                
                if url_col:
                    # Full read for analysis
                    df = pd.read_csv(filepath, usecols=[url_col], encoding='utf-8-sig', on_bad_lines='skip', low_memory=False)
                    urls = df[url_col].astype(str).apply(normalize_fb_url).tolist()
                    
                    total_urls = len(urls)
                    bot_hits = sum(1 for u in urls if u in self.known_bots)
                    
                    results.append({
                        'File': filepath,
                        'Total URLs': total_urls,
                        'Bot Hits': bot_hits,
                        'Saturation': (bot_hits / total_urls * 100) if total_urls else 0
                    })
             except Exception:
                 continue

        if results:
            full_df = pd.DataFrame(results)
            full_df = full_df.sort_values(by='Bot Hits', ascending=False)
            full_df.to_csv("Investigative_Archive/File_Level_Bot_Report.csv", index=False, encoding='utf-8-sig')
            print(f"[✔] File-Level Bot Report saved to: Investigative_Archive/File_Level_Bot_Report.csv")

    def generate_forensic_script_report(self):
        """
        Scans all user profiles for Foreign Scripts using regex engine. 
        """
        print("[*] Initiating Forensic Script Analysis...")
        forensic_data = []
        
        for url, data in self.user_db.items():
            name = data.get('name', '')
            if not name or name == "nan": continue
            
            primary_script = detect_foreign_script(name)
            
            if primary_script:
                all_detected = set()
                for label, pattern in REGEX_FOREIGN.items():
                    if pattern.search(name): all_detected.add(label)
                
                if primary_script in all_detected: all_detected.remove(primary_script)
                secondary_str = ", ".join(sorted(list(all_detected))) if all_detected else "None"
                
                found_in_posts = list(data.get('evidence_log', {}).keys())
                location_str = " | ".join(found_in_posts)
                
                forensic_data.append({
                    'Name': name,
                    'Profile URL': url,
                    'Primary Script': primary_script,
                    'Secondary Scripts': secondary_str,
                    'Found In Files': location_str,
                    'Total Occurrences': len(found_in_posts)
                })
        
        if forensic_data:
            df = pd.DataFrame(forensic_data)
            df = df.sort_values(by=['Primary Script', 'Total Occurrences'], ascending=[True, False])
            
            output_path = "Investigative_Archive/Forensic_Script_Analysis.csv"
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"[✔] Forensic Script Analysis Report saved.")
        else:
            print("[*] No significant foreign script activity detected.")

    def export_results(self):
        """
        Main export function. Orchestrates the generation of all CSV reports.
        """
        if not os.path.exists("Investigative_Archive"):
            os.makedirs("Investigative_Archive")

        self.generate_file_level_bot_report()
        self.generate_forensic_script_report()
            
        print("[*] Exporting Main Intelligence Registry...")
        export_list = []

        # Party Fingerprints generation for alignment calculation
        party_fingerprints = {}
        for party in set(data.get('primary_affiliation') for data in self.user_db.values() if data.get('is_id_verified')):
            if "Neutral" in party: continue
            party_users = [u for u, d in self.user_db.items() if d.get('primary_affiliation') == party]
            party_targets = Counter()
            for u in party_users:
                party_targets.update(self.user_db[u]['targeted_entities'])
            party_fingerprints[party] = [target for target, count in party_targets.most_common(10)]

        for url, data in self.user_db.items():
            cluster = data.get('cluster_membership', 'Individual')
            party = data.get('primary_affiliation', 'Neutral/Unknown')
            flags = []
            
            media_entries = data.get('targeted_media', {})
            media_str = ", ".join([f"{k}({v})" for k, v in media_entries.items()])

            # Detect Internal Conflict
            if party != "Neutral/Unknown" and party in data['attacked_parties']:
                flags.append(f"FLAG:INTERNAL_CONFLICT_WITH_{party}")
            
            # Identify Key Actors
            if data['hostility_score'] > 30 and any(x in cluster for x in ["SQUAD", "UNIT"]):
                flags.append("HVT:CLUSTER_LEAD")

            support_count = len(data['support_events'])
            attack_count = len(data['attack_events'])
            
            combat_role = "Active Aggressor" if attack_count > (support_count * 2) else "Amplifier"
            if "HVT:CLUSTER_LEAD" in flags:
                combat_role = "COORDINATOR"

            conflict_target = "None"
            if party != "Neutral/Unknown":
                conflicts = []
                for target_party, ac in data['attacked_parties'].items():
                    if target_party not in [party, 'MEDIA', 'JOURN'] and ac > 3:
                        conflicts.append(f"{target_party}({ac})")
                if conflicts:
                    conflict_target = " | ".join(conflicts)
                    flags.append(f"CONFLICT:{conflict_target}")
            
            # Use Dynamic Thresholds
            is_elite = "Yes" if data['hostility_score'] >= self.elite_threshold else "No"
            is_high_freq = "Yes" if len(data['attack_events']) > self.high_freq_threshold else "No"
            is_tactical = "Yes" if combat_role in ["Active Aggressor", "COORDINATOR", "Amplifier"] else "No"
            
            if is_elite == "Yes": flags.append("ELITE_ACTOR")
            if is_high_freq == "Yes": flags.append("HIGH_FREQUENCY")
            if is_tactical == "Yes": flags.append("TACTICAL_CLUSTER_MEMBER")

            j_hits = data['attacked_parties'].get('JOURN', 0)
            m_hits = data['attacked_parties'].get('MEDIA', 0)
            if j_hits > 0: flags.append(f"JOURN_HITS:{j_hits}")
            if m_hits > 0: flags.append(f"MEDIA_HITS:{m_hits}")

            is_bot = "Yes" if url in self.known_bots else "No"

            export_list.append({
                'Profile URL': url,
                'Is Known Bot': is_bot,
                'Name': data['name'],
                'Primary Affiliation': party,
                'Behavioral Alignment': data.get('behavioral_alignment', 'N/A'),
                'Cluster ID': cluster,
                'Role': combat_role,
                'Hostility Score': data['hostility_score'],
                'Flag: Elite Actor': is_elite,
                'Flag: High Frequency': is_high_freq,
                'Flag: Tactical Cluster': is_tactical,
                'Flag: Journalist Hits': j_hits,
                'Flag: Media Hits': m_hits,
                'Flag: Conflict Target': conflict_target,
                'Total Attack Events': len(data['attack_events']),
                'Total Support Events': len(data['support_events']),
                'Target Mix': ", ".join([f"{k}:{v}" for k, v in data['attacked_parties'].items()]),
                'Targeted Entities': ", ".join([f"{k}({v})" for k, v in data.get('targeted_entities', {}).items()]),
                'Targeted Media': media_str,
                'Event History': " | ".join(list(data['attack_events'])), 
                'Intelligence Flags': " | ".join(flags)
            })

        df = pd.DataFrame(export_list)
        df.sort_values(by=['Cluster ID', 'Hostility Score'], ascending=[True, False], inplace=True)
        df.to_csv("Investigative_Archive/Knightfall_V5_Intelligence_Report.csv", index=False, encoding='utf-8-sig')

        # Known Bot Cross-Reference Report
        print("[*] Generating Confirmed Bot Interventions Report...")
        confirmed_bot_hits = []
        for url, data in self.user_db.items():
            if url in self.known_bots:
                confirmed_bot_hits.append({
                    'Bot Name': data.get('name', 'Unknown'),
                    'Profile URL': url,
                    'Cluster Membership': data.get('cluster_membership', 'Unassigned'),
                    'Behavioral Alignment': data.get('behavioral_alignment', 'N/A'),
                    'Total Attacks': sum(data['targeted_entities'].values()),
                    'Victims Targeted': ", ".join(list(data['targeted_entities'].keys())),
                    'Found In Files': " | ".join(list(data.get('evidence_log', {}).keys()))
                })

        if confirmed_bot_hits:
            bot_df = pd.DataFrame(confirmed_bot_hits)
            bot_df.to_csv("Investigative_Archive/Confirmed_Bot_Interventions.csv", index=False, encoding='utf-8-sig')
            print(f"[✔] ALERT: Found {len(bot_df)} confirmed bots active in this dataset.")
        else:
            print("[*] No known bots found in the current dataset.")

    def generate_master_attack_stats(self):
        """
        Generates Master_Attack_Stats_Integrated.csv summarizing party affiliation counts per file.
        """
        print("[*] Generating Master Attack Stats CSV...")
        results = []
        
        sorted_files = sorted(self.file_participants.keys())
        for filename in sorted_files:
            participants = self.file_participants[filename]
            counts = Counter()
            bot_count = 0
            
            for url in participants:
                if url in self.known_bots: bot_count += 1
                
                if url in self.user_db:
                    aff = self.user_db[url].get('primary_affiliation', 'Neutral/Unknown')
                    aff_clean = aff.replace("_", " ")
                    counts[aff_clean] += 1
                else:
                    counts['Neutral/Unknown'] += 1
            
            row = {'Filename': filename, 'Confirmed Bots': bot_count}
            row.update(counts)
            results.append(row)
            
        if results:
            df = pd.DataFrame(results).fillna(0)
            cols = [c for c in df.columns if c != 'Filename']
            df[cols] = df[cols].astype(int)
            
            other_cols = [c for c in sorted(cols) if c != 'Confirmed Bots']
            final_cols = ['Filename', 'Confirmed Bots'] + other_cols
            df = df[[c for c in final_cols if c in df.columns]]
            
            df.to_csv("Master_Attack_Stats_Integrated.csv", index=False, encoding='utf-8-sig')
            print("[*] Master Attack Stats saved.")

    def generate_master_squad_stats(self):
        """
        Generates Master_Squad_Stats.csv summarizing cluster/squad membership counts per file.
        """
        print("[*] Generating Master Squad Stats CSV...")
        results = []
        sorted_files = sorted(self.file_participants.keys())
        
        for filename in sorted_files:
            participants = self.file_participants[filename]
            counts = Counter()
            
            for url in participants:
                if url in self.user_db:
                    squad = self.user_db[url].get('cluster_membership', 'Individual')
                    counts[squad] += 1
                else:
                    counts['Unknown'] += 1
            
            row = {'Filename': filename}
            row.update(counts)
            results.append(row)
            
        if results:
            df = pd.DataFrame(results).fillna(0)
            cols = [c for c in df.columns if c != 'Filename']
            df[cols] = df[cols].astype(int)
            
            special_cols = ['Individual', 'Small_Cell', 'Unknown']
            squad_cols = sorted([c for c in cols if c not in special_cols])
            final_cols = ['Filename'] + squad_cols + [c for c in special_cols if c in df.columns]
            
            df = df[[c for c in final_cols if c in df.columns]]
            df.to_csv("Master_Squad_Stats.csv", index=False, encoding='utf-8-sig')
            print("[*] Master Squad Stats saved.")

    def analyze_bot_overlaps(self):
        """
        [BOT INTERSECTION ANALYSIS]
        Analyzes overlap between different bot networks (Commercial vs Political vs Foreign).
        """
        print("\n" + "="*60)
        print("BOT OVERLAP ANALYSIS: CROSS-REFERENCE")
        print("="*60)
        
        bot_profiles_dir = "Bot Profiles"
        if not os.path.exists(bot_profiles_dir):
            print(f"[!] Bot Profiles directory not found.")
            return

        commercial_bots = {} 
        candidate_bots = {}
        foreign_bots = {}
        
        print("[*] Scanning Bot Profiles...")
        files = [f for f in os.listdir(bot_profiles_dir) if f.endswith('.csv')]
        
        for f in files:
            path = os.path.join(bot_profiles_dir, f)
            try:
                # Engine 'python' needed for robustness against bad lines in some CSVs
                df = pd.read_csv(path, engine='python', on_bad_lines='skip')
                
                url_col = next((c for c in df.columns if 'profile url' in c.lower()), None)
                if not url_col: continue
                
                name_col = next((c for c in df.columns if 'name' in c.lower()), 'Name')
                df['NormURL'] = df[url_col].astype(str).apply(normalize_fb_url)
                
                records = df[[name_col, 'NormURL', url_col]].to_dict('records')
                
                if "Foreign" in f:
                    for r in records:
                        idx = r['NormURL']
                        if idx not in foreign_bots:
                            foreign_bots[idx] = {'Name': r[name_col], 'Sources': Counter(), 'OriginalURL': r[url_col]}
                        foreign_bots[idx]['Sources'][f] += 1
                        
                elif f.lower().startswith("bot_"):
                    service_name = f[4:].replace(".csv", "")
                    for r in records:
                        idx = r['NormURL']
                        if idx not in commercial_bots:
                            commercial_bots[idx] = {'Name': r[name_col], 'Sources': Counter(), 'OriginalURL': r[url_col]}
                        commercial_bots[idx]['Sources'][service_name] += 1
                        
                else:
                    candidate_name = f.replace(".csv", "")
                    for r in records:
                        idx = r['NormURL']
                        if idx not in candidate_bots:
                            candidate_bots[idx] = {'Name': r[name_col], 'Sources': Counter(), 'OriginalURL': r[url_col]}
                        candidate_bots[idx]['Sources'][candidate_name] += 1
                        
            except Exception as e:
                print(f"[!] Error reading {f}: {e}")

        def format_sources(source_counter):
            return ", ".join([f"{k} ({v})" for k, v in sorted(source_counter.items())])

        def generate_overlap_report(set_a, set_b, label_a, label_b, filename):
            common_urls = set(set_a.keys()) & set(set_b.keys())
            data = []
            for url in common_urls:
                info_a = set_a[url]
                info_b = set_b[url]
                data.append({
                    'Profile URL': info_a['OriginalURL'], 
                    'Bot Name': info_a['Name'],
                    f'{label_a} Source': format_sources(info_a['Sources']),
                    f'{label_b} Source': format_sources(info_b['Sources']),
                    'Normalized URL': url
                })
            
            if data:
                df_out = pd.DataFrame(data)
                df_out.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"    - Generated {filename}: {len(df_out)} overlaps")
            return data

        # Generate Overlap CSVs
        generate_overlap_report(commercial_bots, candidate_bots, "Commercial", "Candidate", "Overlap_Commercial_Candidate.csv")
        generate_overlap_report(commercial_bots, foreign_bots, "Commercial", "Foreign", "Overlap_Commercial_Foreign.csv")
        
        # Candidate-Candidate Overlap
        cand_overlap_data = []
        for url, info in candidate_bots.items():
            if len(info['Sources']) > 1:
                cand_overlap_data.append({
                    'Profile URL': info['OriginalURL'],
                    'Bot Name': info['Name'],
                    'Shared By Candidates': format_sources(info['Sources']),
                    'Source Count': len(info['Sources'])
                })
        
        if cand_overlap_data:
            df_cc = pd.DataFrame(cand_overlap_data)
            df_cc = df_cc.sort_values(by='Source Count', ascending=False)
            df_cc.to_csv("Overlap_Candidate_Candidate.csv", index=False, encoding='utf-8-sig')
            print(f"    - Generated Overlap_Candidate_Candidate.csv: {len(df_cc)} shared bots.")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    print_scientific_disclaimer()
    engine = KnightfallArchitect()
    engine.run_parallel_ingestion()
    engine.resolve_affiliations()
    engine.detect_coordinated_clusters()
    engine.generate_chord_diagram()
    engine.generate_master_attack_stats()
    engine.generate_master_squad_stats()
    engine.analyze_bot_overlaps()
    engine.export_results()
    print("\n[SUCCESS] Knightfall Core Engine extraction complete.")
