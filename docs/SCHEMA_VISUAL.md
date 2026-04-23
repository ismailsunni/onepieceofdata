# One Piece Database Visual Schema

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ONE PIECE DATABASE SCHEMA                             │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│      SAGA       │    │      ARC        │    │     CHAPTER     │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ saga_id (PK)    │◄─┐ │ arc_id (PK)     │    │ number (PK)     │
│ title           │  └─┤ saga_id (FK)    │◄─┐ │ title           │
│ start_chapter ──┼────┼─start_chapter   │  └─┤ volume (FK)     │
│ end_chapter ────┼────┼─end_chapter     │    │ num_page        │
│ description     │    │ title           │    │ date            │
└─────────────────┘    │ description     │    │ jump            │
                       └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                                               ┌─────────────────┐
                                               │      COC        │
                                               │ (Character of   │
                                               │    Chapter)     │
                                               ├─────────────────┤
                                               │ chapter (FK) ───┼─┐
                                               │ character       │ │
                                               │ note            │ │
                                               └─────────────────┘ │
                                                       ▲           │
                                                       │           │
┌─────────────────┐    ┌─────────────────┐            │           │
│     VOLUME      │    │   CHARACTER     │◄───────────┘           │
├─────────────────┤    ├─────────────────┤                        │
│ number (PK) ────┼────┼─id (PK)         │                        │
│ title           │    │ name            │                        │
└─────────────────┘    │ bounty          │                        │
                       │ status          │                        │
                       │ scraping_status │                        │
                       └─────────────────┘                        │
                                  ▲                               │
                                  └───────────────────────────────┘

RELATIONSHIPS:
├── SAGA → ARC (1:Many)         : One saga contains multiple arcs
├── ARC → CHAPTER (Range)       : Arcs span chapter ranges
├── VOLUME → CHAPTER (1:Many)   : One volume contains multiple chapters
├── CHAPTER ↔ CHARACTER (M:M)   : Via COC junction table
└── SAGA → CHAPTER (Range)      : Sagas span chapter ranges

DATA COUNTS:
├── Sagas: ~10-15 major story arcs
├── Arcs: ~50+ individual story arcs  
├── Volumes: 112+ published volumes
├── Chapters: 1,153+ published chapters
├── Characters: 1,533+ unique characters
└── Relations: 25,708+ character appearances

SAMPLE QUERIES:

1. Get all arcs in East Blue saga:
   SELECT title FROM arc WHERE saga_id = 'east_blue';

2. Find Luffy's appearances:
   SELECT chapter FROM coc WHERE character LIKE '%Luffy%';

3. Count chapters per volume:
   SELECT volume, COUNT(*) FROM chapter GROUP BY volume;

4. Get saga progression:
   SELECT title, start_chapter, end_chapter 
   FROM saga ORDER BY start_chapter;
```
