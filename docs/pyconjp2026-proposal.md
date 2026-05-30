# PyCon JP 2026 — Proposal & Talk Plan

Working document for the *One Piece of Data* submission to PyCon JP 2026.

---

## Submission summary

- **Title:** One Piece of Data: Enjoying One Piece with Python
- **Track:** Data Engineering and Analysis
- **Audience level:** Intermediate
- **Format:** 30 minutes (open to 45 if offered)
- **CfP deadline:** May 31, 2026 (AoE)

---

## Proposal fields

### Abstract (≤800 chars)

> What happens when a software engineer is also a hardcore One Piece fan? You ask your favorite manga the questions a re-read can't answer — like who's the rumored 12th Straw Hat — and reach for Python. The clues are out there: One Piece runs 1,180+ chapters, its secrets buried in a messy fandom wiki.
>
> I'll show how I built [onepieceofdata.com](https://onepieceofdata.com): a pipeline of scrapers, parsers, and post-processors that turns the wiki into Pydantic-validated data in DuckDB. Being a fan shaped every design choice — deduplicating disguised characters, modeling structure the source never states — in a solid pipeline I re-run weekly, powering analytics, a character network, a story graph, a chatbot, and data-driven Instagram reels — all chasing what every fan asks: when will we find the One Piece?

*(FINAL — author's locked version, with light grammar polish applied. 782 plain-text chars. Two-act structure: fan-engineer + messy-data problem, then the scraper → parser → post-processor pipeline (Pydantic, DuckDB), the explicit thesis ("being a fan shaped every design choice"), weekly-rerun robustness, the five live outputs (analytics, character network, story graph, chatbot, data-driven Instagram reels — ismailsunni.id/onepieceofdata-reels), and the double-meaning close "when will we find the One Piece?")*

#### Leaner alternative (795 chars — focused on the core pipeline, no AI/graph)

> What happens when a software engineer is also a hardcore fan? You ask your favorite manga questions you can't answer by re-reading it — and you reach for Python.
>
> One Piece, the famous pirate manga, is 1,180+ chapters long and still going. The questions never stop: when will it end? Who's the rumored 12th Straw Hat? Are chapters really getting more complex? The clues are buried in a messy fandom wiki.
>
> I'll show how I built onepieceofdata.com: a pipeline of scrapers, parsers, and post-processors that turns the wiki into Pydantic-validated data in DuckDB, served via a public site. The fun is where fandom meets engineering — deduplicating disguised characters, computing story structure the wiki never states, and turning 25,000+ appearances into charts that answer what fans actually ask.

*(May still consider adding a "who this is for" closing line, inspired by the kakigori talk.)*

### Speaker experience (≤500 chars)

> I'm a software engineer at Camptocamp in open-source geospatial, contributing to Swiss Geo Admin; a QGIS contributor, founder of QGIS Indonesia, and open-source maintainer on [GitHub](https://github.com/ismailsunni). I founded Python Jogja in 2016, organize Python Indonesia, and have spoken at FOSS4G and QGIS conferences. One Piece of Data is my solo side project — a production Python pipeline at onepieceofdata.com — and I've already given a fan version of this talk to a 200+ person One Piece meetup.

### Discussion / engagement plans (≤500 chars)

> The conversation I most want to spark: how much does domain expertise shape a data project? How different would One Piece of Data look if I weren't a fan — and when has your own domain knowledge changed a technical decision? And the messy part: how do you force ambiguous, subjective reality into a clean schema — when one character has three identities, or a relationship the source never states outright? Where do you draw the line? What would you build, or want to see, on onepieceofdata.com?

---

## Talk thesis

**Domain expertise shapes data engineering decisions in ways tools alone can't replicate.**

Everything in the talk serves this thesis. The web app is *evidence*, not subject — proof the data is useful, mentioned for ~30 seconds total.

---

## 30-minute outline

| Time | Section | Beat |
|---|---|---|
| 0:00 | Hook | The question I couldn't answer by re-reading — and the live site, in 15 seconds |
| 2:00 | Thesis | "Domain expertise shaped every schema decision" — preview |
| 3:00 | Source | The Fandom wiki: gold + swamp. One messy infobox screenshot |
| 5:00 | Scraping | Parallel workers, retries, status tracking. One concrete failure mode I designed around |
| 8:00 | **Modeling (heart of the talk)** | Character dedup → arc/saga auto-linking → "appearance" types. Three vignettes, ~2 min each |
| 14:00 | Storage & serving | DuckDB + Pydantic as the contract; export to Supabase, schema-mapping gotchas. Show the public site for ~30s |
| 17:00 | Network & story graph | Character co-appearance graph (network explorer) + LLM-extracted story-graph triples. The fan questions a graph answers that a table can't |
| 20:00 | The chatbot | RAG over the data: SQL + vector search tools. Honest take — where it shines, where data gaps make it fail (a fan spots them instantly) |
| 23:00 | Operations (living story) | `make update-new-chapter` weekly. A still-running manga means the pipeline is never "done". What breaks, how I find out |
| 25:00 | Honest lessons | What worked, what didn't, side projects don't need to be perfect |
| 27:00 | Close + Q&A | Three invitations from the engagement plans field |

---

## What to spotlight (the 80%)

1. **Schema designed around fan questions, not source structure.** Show one schema decision that's not obvious from the source.
2. **Character deduplication** (Mr. 1 = Daz Bonez, Akainu = Sakazuki, Lucy = Sabo). Pure domain knowledge encoded into an aliases file. Funny and technical.
3. **Computed vs. scraped relationships.** Arc-to-saga linking computed from chapter ranges. "The source doesn't have this, but the data implies it."
4. **Different kinds of "appearance."** Chapter / cover / cameo — non-fan would flatten, you didn't.
5. **Scraping at scale with grace.** Parallel workers, retries, error tracking, rate limits.
6. **DuckDB → Supabase pipeline.** Schema mapping (INTEGER[] arrays), incremental vs full export.
7. **The graph layer.** Character co-appearance network (from CoC) + LLM-extracted story-graph triples. A question a graph answers that a flat table can't — e.g. "who connects two crews?"
8. **The chatbot, honestly.** RAG over the same data (SQL + vector-search tools). Where it works and where data gaps make it fail — a fan catches those instantly; that's the thesis again.
9. **Weekly refresh / living story.** `make update-new-chapter`. The data is never "done" because the manga isn't. What breaks, how I find out.

## Brief mentions (the 15%)

- Pydantic models as scraper↔DB contract — 1 slide
- Quality tracking / status codes (97.7% clean) — 1 bullet

## ~30 seconds total (the 5%)

- The web app — one screenshot, "and here's what it enables, onepieceofdata.com"
- Community / fun-side-project closing message

## Out entirely

- React, Vite, Netlify, hosting stack
- AI coding assistants (unless asked in Q&A)
- v1 → v2 migration history
- Detailed library tour
- Python packaging deep dive

---

## Preparation checklist

- [ ] **Live site demo** — practice it offline-capable; conference WiFi will fail
- [ ] **One real screenshot** of a messy wiki infobox
- [ ] **The alias file** as a slide (`character_aliases.json` excerpt)
- [ ] **One Pydantic model** as a slide — pick the most readable one
- [ ] **One architecture diagram** — scraper → DuckDB → Supabase → React. Single slide
- [ ] **Two "answered question" charts** — generate ahead so you don't depend on live queries on stage
- [ ] **One network-graph visual** — character co-appearance graph (network explorer screenshot or short clip), pre-rendered
- [ ] **One chatbot exchange** — a screenshot showing a good answer *and* a telling failure (data gap), captured ahead of time
- [ ] **A "queries to try" gist or page** for after-talk exploration (optional, low effort)
- [ ] **One funny moment** — character disguises are a natural fit

---

## Discipline rule

For every slide, ask: *"Does this serve the fan-engineer story, or am I just listing things I'm proud of?"* If the latter, cut it. A 30-min talk has room for ~20 slides max — every one must earn its spot.

---

## Open questions / future revisits

- Final abstract version — two are kept: the feature-rich one (primary) and the focused core-pipeline variant. Decide which to submit.
- Add an explicit "who this is for" line to the abstract, kakigori-style?
- Which two fan questions to feature in the hook and the payoff (sections 2 and 6)?
- AI-as-tool one-liner ("AI couldn't tell me Mr. 1 was Daz Bonez") — include or skip?
- Outline is now ~6 min tighter on Modeling to fit the graph + chatbot beats. If the talk runs long in rehearsal, the graph and chatbot are the first candidates to merge into a single "what the data enables" segment — and the focused abstract variant matches that fallback.
- Second proposal slot — leave empty, or pitch the Python Jogja / community angle separately?

---

## Track choice reasoning (locked: Data Engineering and Analysis)

Substance honesty — the talk is *about* data engineering. Reviewers in any other track would think "why didn't this go to Data Engineering?" Camptocamp + Swiss Geo Admin credentials reinforce the placement. Differentiation comes from *within* the track: most data engineering talks are about work systems; this one is a personal data product shipped to the public.

Community/Education flavor lives in the engagement plans, not the track.
