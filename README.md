# Applications of Artificial Intelligence — CM500329
**University of Bath — Module 2**

Two graded assignments covering data engineering fundamentals and NLP classification, progressing from a hand-built CSV parser through to transformer fine-tuning with LoRA.

---

## Assignment 1: Data Ingestion & Wrangling

### 1.1 — Custom RFC 4180 CSV Parser

Built from scratch with no CSV libraries (no pandas, no numpy, no `csv` module) — a generator-based parser that processes files line-by-line for memory efficiency.

**Key features:**
- RFC 4180 compliant multi-line quoted field handling
- Automatic type inference: int, float, datetime, string with precedence logic
- Intelligent header detection via numeric vs text heuristics
- Date format ambiguity detection (DD/MM vs MM/DD)
- Graceful error handling for malformed files
- BOM removal for UTF-8 encoding

**Test data:** Four UK weather datasets from 2016–17 (rainfall, barometric pressure, indoor/outdoor temperature — ~350 records each).

### 1.2 — Statistical Analysis & Data Corruption Study

- Min, max, mean, standard deviation calculator built on top of the custom parser
- Introduced four realistic corruptions into the temperature dataset (sensor malfunction, logical inconsistency, transmission error, clustered failure)

**Finding:** Basic descriptive statistics were largely resistant to sparse outliers — the mean shifted only 1% while min/max changed by up to 1,497%. Conclusion: visual time-series analysis is necessary alongside summary statistics for anomaly detection.

---

## Assignment 2: Sentiment Classification

### 2.1 — Naive Bayes Baseline

**Dataset:** 1,382 car reviews (binary: Positive / Negative, balanced classes)

**Pipeline:**
- Lowercase, punctuation removal, NLTK stopwords, Porter stemming
- Bag-of-words vectorisation (training vocabulary only — no data leakage)
- Multinomial Naive Bayes with 5-fold GridSearchCV over Laplace smoothing values [0.1 → 10.0]
- Evaluated via confusion matrix, precision, recall, F1

**Limitations documented:** word order ignored, negation unhandled ("not good" treated same as "good"), no semantic relationships.

### 2.2 — Transformer Fine-Tuning with LoRA

Improved the baseline using a pre-trained transformer with parameter-efficient fine-tuning.

| | Naive Bayes | Transformer + LoRA |
|--|------------|-------------------|
| Model | Multinomial NB | Longformer (4096 ctx) |
| Features | Bag-of-words | Contextual embeddings |
| Negation handling | None | Via self-attention |
| Word order | Ignored | Preserved |
| Parameters trained | All | LoRA adapters only |

**LoRA** (Low-Rank Adaptation) fine-tunes a small number of adapter parameters rather than the full model — preserving pre-trained knowledge while adapting to the sentiment task efficiently.

---

## Key Concepts Covered

- RFC-compliant data parsing and type inference
- Statistical anomaly detection and its limitations
- NLP preprocessing pipeline (tokenisation, stemming, stopword removal)
- Train/test separation and data leakage prevention
- Hyperparameter tuning with cross-validation
- Transformer architecture and contextual embeddings
- Parameter-efficient fine-tuning (LoRA)
- Classification metrics: confusion matrix, precision, recall, F1, ROC/AUC

---

## Stack

`Python` · `NLTK` · `scikit-learn` · `Hugging Face Transformers` · `PyTorch` · `matplotlib` · `seaborn`

---

## Course Context

**Institution:** University of Bath  
**Module:** CM500329 — Applications of Artificial Intelligence  
**Weighting:** Assignment 1 (30%) · Assignment 2 (30%) · Exam (40%)
