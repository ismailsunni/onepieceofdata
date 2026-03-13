# Character Co-Appearance Network Analysis (appearance_count > 10)

## Data Source
- DuckDB: `data/onepiece.duckdb`
- Chapter-character table: `coc`
- Character filter: `character.appearance_count > 10`

## Network Definition
- Node: a character with more than 10 chapter appearances
- Edge: two characters appear in the same chapter at least once
- Edge weight: number of chapters where the pair co-appears

## Summary Metrics
- Eligible characters (nodes): **510**
- Co-appearance edges: **41174**
- Chapters contributing at least one pair: **1176**
- Maximum pair co-appearance weight: **762**

## Top 15 Characters by Weighted Degree

```
                    id                   name  appearance_count  weighted_degree  degree  betweenness
       Monkey_D._Luffy        Monkey D. Luffy              1001            19160     509          0.0
                  Nami                   Nami               821            15876     509          0.0
          Roronoa_Zoro           Roronoa Zoro               786            15239     509          0.0
                 Sanji                  Sanji               765            15218     509          0.0
                 Usopp                  Usopp               753            15073     509          0.0
     Tony_Tony_Chopper      Tony Tony Chopper               694            14539     508          0.0
            Nico_Robin             Nico Robin               600            12919     503          0.0
                Franky                 Franky               472            11013     492          0.0
                 Brook                  Brook               398             9603     473          0.0
                 Jinbe                  Jinbe               283             7210     461          0.0
Trafalgar_D._Water_Law Trafalgar D. Water Law               207             5401     341          0.0
              Kin'emon               Kin'emon               172             4759     362          0.0
    Kouzuki_Momonosuke     Kouzuki Momonosuke               139             3904     360          0.0
      Charlotte_Linlin       Charlotte Linlin               139             3731     330          0.0
 Donquixote_Doflamingo  Donquixote Doflamingo               124             3192     323          0.0
```

## Top 15 Strongest Co-Appearance Pairs

```
      source_name       target_name  weight
  Monkey D. Luffy              Nami     762
  Monkey D. Luffy      Roronoa Zoro     733
  Monkey D. Luffy             Sanji     712
  Monkey D. Luffy             Usopp     689
             Nami             Sanji     675
             Nami      Roronoa Zoro     660
             Nami             Usopp     654
     Roronoa Zoro             Usopp     649
  Monkey D. Luffy Tony Tony Chopper     636
             Nami Tony Tony Chopper     628
     Roronoa Zoro             Sanji     615
            Sanji             Usopp     613
            Sanji Tony Tony Chopper     595
     Roronoa Zoro Tony Tony Chopper     553
Tony Tony Chopper             Usopp     549
```
