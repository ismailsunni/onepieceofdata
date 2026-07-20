# One Piece of Data: Enjoying One Piece with Python

🌐 Project: **[onepieceofdata.com](https://onepieceofdata.com)**

---

## Abstract

What happens when a software engineer is also a hardcore fan? You start asking your favorite manga questions you can't answer by reading it again — and you reach for Python.

**One Piece** is the best-selling manga in history — over 500 million copies, 1,165+ chapters across 27 years, 113+ volumes, and a sprawling cast of pirates, marines, and revolutionaries. For fans, the questions never stop:

- **When will One Piece finally end?**
- **Who is the rumored 12th member of the Straw Hat Pirates?**
- **Is there a hidden pattern between a character's origin or blood type and their power level?**
- **How are the major characters connected — by crew, by bloodline, by rivalry?**
- **Have chapters genuinely grown more complex over time, or is that just nostalgia?**

The fandom wiki has clues for all of these — buried in messy, inconsistent, free-form text. In this talk, I will share how I built **One Piece of Data** ([onepieceofdata.com](https://onepieceofdata.com)), a Python project that turns that messy fandom knowledge into structured, story-driven insights anyone can explore.

This talk is about the real-world journey: unreliable sources, evolving design decisions, and the surprising importance of being a fan.

---

## Why Me / My Experience

I am a software engineer working with data processing systems, and a dedicated One Piece fan for many years.

This combination is essential for this project:

- My **technical background** lets me build practical Python pipelines.
- My **domain knowledge** helps me detect inconsistencies, validate edge cases, and design data models that match how fans actually think about the story.
- My **passion** drives continuous iteration far beyond a typical side project.

Through One Piece of Data, I have:

- Built an end-to-end pipeline for over a thousand chapters of messy, evolving data.
- Iterated through several frontends to find the right way to tell the story.
- Shipped a public site, [onepieceofdata.com](https://onepieceofdata.com), that other fans actually use.

---

## What I Will Cover

### 1. The One Piece Universe — A Perfect (and Painful) Dataset

- 27 years of weekly chapters, hundreds of named characters, dozens of arcs and sagas, an entire fictional geography.
- Why fandom data is uniquely hard: subjective boundaries, conflicting sources, inconsistent naming, evolving canon.
- A few fun stats and surprises from the project to set the tone.

### 2. Investigating the Fan Questions with Python

The heart of the talk — using the data to explore real fan debates:

- **When will One Piece end?** Modeling chapter pacing, arc length, and remaining plot threads.
- **The 12th Straw Hat.** What does the data say about "important characters"?
- **Power vs. origin / blood type.** Is there a pattern, or is it apophenia? (Spoiler: a null result is still a result.)
- **Character relationship graphs.** Who is connected to whom, and how densely?
- **Story complexity over time.** Are chapters genuinely getting denser, or do we just remember Marineford too fondly?

I'll walk through the actual analyses — queries, transformations, charts — and show how each question shaped the data model.

### 3. Why Domain Knowledge Matters

- Being a fan isn't a bonus — it's a requirement. Without it, the data is just noise.
- Examples where the *technically correct* answer is the *wrong* answer:
  - Arc-to-saga boundaries — the wiki disagrees with itself; chapter ranges resolve it.
  - Character appearances: cover art vs. in-chapter cameos vs. flashbacks.
  - Aliases, epithets, and bounty changes for the same character over time.
- The lesson: the person who *understands* the domain beats the person who just *processes* it.

### 4. The Reality of Data in the Wild

- A wiki written by hundreds of contributors over two decades.
- Missing fields, broken templates, contradictory facts.
- Handling ambiguity without pretending it isn't there.

### 5. Designing a Practical Python Pipeline

- I'll show the architecture in a single diagram — **scrape → clean → shape → serve** — and then spend the time on the *decisions* behind it, not the boxes.
- Parallel scraping that survives a flaky wiki.
- Schema design driven by domain insight (auto-linking arcs to sagas, denormalizing appearance data).
- The weekly-update problem: a new chapter drops every Sunday — how do you re-run the whole thing reliably, solo?
- Avoiding overengineering — this is a side project, not a platform.

### 6. When Python Isn't Enough

- I started with Streamlit, then tried Evidence, and eventually moved the frontend out of the Python ecosystem entirely.
- Why each switch happened — and when you *shouldn't* switch.
- The punchline: **Python still does all the heavy lifting** — analysis, charts, animated GIFs, graph data. The frontend is just a window.

### 7. Lessons Learned

- Tools are temporary; **data design is long-term**.
- A good schema survives three frontends. A bad one doesn't survive one.
- Balancing engineering quality with creative output as a solo maintainer.
- *(Brief aside: I also explored a RAG chatbot over the wiki. Still rough — happy to discuss in Q&A.)*

---

## What I Want to Discuss with Attendees

This will be an interactive session. I'll include discussion prompts and invite attendees to share their own experiences:

- How much domain knowledge is "enough" in a data project?
- When should you move beyond tools like Streamlit?
- What questions would *you* ask of a dataset you love?
- Trade-offs between speed, flexibility, and scalability.

---

## Target Audience

- **Python developers** working with data pipelines.
- **Engineers** building side projects or data-driven products.
- **Creators** interested in combining data with storytelling.
- **One Piece fans and manga readers** curious about what data can reveal about the stories they love — no coding background needed to enjoy this part.

---

## Key Takeaways

- A practical approach to building Python data pipelines around messy real-world data.
- Why domain knowledge is often the most underrated skill in data work.
- Real lessons from evolving a personal project over time.
- Inspiration to turn personal passion into meaningful technical projects — and a live site at [onepieceofdata.com](https://onepieceofdata.com) to explore afterward.

---

## Additional Notes

This talk is based on an ongoing personal project driven by both technical curiosity and fandom. Both repositories are open source, and the live site is at **[onepieceofdata.com](https://onepieceofdata.com)**. I'll share not just what worked, but what failed — and why the failures mattered more.
