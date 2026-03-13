                            *Knightfall Core Engine - Documentation*

**Executive Summary:**

Knightfall is a forensic graph-intelligence script designed to detect ‘***Strike Squads***’ \- coordinated groups engaging in systematic attacks or support across unrelated events. It is ideally suited for identifying "astroturfing" and algorithmic manipulation by distinguishing organic viral behavior from manufactured coordination.

**Scientific Methodology & Limitations**  
The script uses probabilistic modeling to infer coordination. It explicitly addresses four critical forensic gaps:

| Critical Gap | Engine Counter-Measure |
| :---- | :---- |
| **Coincidence vs. Coordination** (The "Viral Problem") | **Inverse Frequency Weighting (1/sqrt(N)):** Links formed on massive viral posts are mathematically suppressed. Only patterned overlaps across *multiple* unrelated events trigger a coordination signal. |
| **Dynamic Threshold Bias** (The "Top 5% Problem") | **Dynamic Statistical Percentiles:** The engine calculates thresholds at runtime (e.g., 95th percentile). It identifies *comparative* outliers relative to the specific dataset's intensity rather than using hardcoded limits. |
| **Forensic Attribution** (Script Detection) | **Priority Hierarchy:** Uses a weighted regex hierarchy to filter common false positives (e.g., diaspora populations) from high-probability click-farm indicators (e.g., Burmese, Cyrillic). |
| **Behavioral Noise** (Random Co-reaction) | **Leiden Modularity Optimization:** The graph clustering algorithm ignores weak random connections, solidifying only those groups that are mathematically denser than random chance. |

**Input Data Specification:**  
Strict Filename Convention Required:  
TYPE\_PARTY\_PAGE\_POST.csv

| Component | Description | Examples |
| :---- | :---- | :---- |
| **TYPE** | Interaction nature. | ID (Support), TGT (Attack), BTL (Battleground) |
| **PARTY** | Affiliation of actor or target. | AL, BNP, JAM, NCP, JOURN |
| **PAGE** | Page/Group name. | The Daily Star, The Daily News |
| **POST** | Event identifier. | Post1, VideoLaunch |

*.*  
**Configuration & Tuning:**  
The engine uses Dynamic Statistical Thresholding by default, meaning it adapts to the intensity of your dataset (small vs. large).

***How Runtime Calibration Works:***  
**Elite Hostility:** Automatically sets the cut-off at the 95th percentile of all user hostility scores.  
**Coordination Weight:** Automatically sets the link strength cut-off at the 90th percentile of all graph edges.  
***Manual Override:***  
To force specific sensitivity levels (e.g., for standardizing reports across different months), edit the KnightfallConfig class in Knightfall\_Core\_Engine.py

**Quick Start:**  
1\. *Install Dependencies:*  
    pip install pandas python-igraph leidenalg holoviews bokeh  
      
2\.  *Deploy:* Place \`Knightfall\_Core\_Engine.py\` and your CSV data in the same root folder.  
3\.  *Run:* \`python Knightfall\_Core\_Engine.py\`

**Output Interpretation (\`Investigative\_Archive/\`):**

***\*  \`Strategic\_Chord\_Diagram.html\`:*** Visualizes ***Cross-Faction Bridges*** \- revealing where opposing factions surprisingly successfully coordinate to attack the same target.  
***\*   \`Forensic\_Script\_Analysis.csv\`:*** Highlights accounts using foreign scripts (e.g., Burmese, Cyrillic) which are strong indicators of commercial click farms or hired bot networks.  
***\*   \`File\_Level\_Bot\_Report.csv\`:*** A quick audit showing the percentage of confirmed "Bot Profiles" present in each dataset. (Pre-determined Bot Database is required)  
***\*   Console Output:*** Identifies specific clusters (AL\_STRIKE\_SQUAD\_1\`) and flags \`ELITE\` units (Top 5% hostile).

***Disclaimer: This tool utilizes probabilistic modeling. "Foreign Script" flags or "High Coordination" scores are leads for investigation, not absolute proof of attribution/origin.***

