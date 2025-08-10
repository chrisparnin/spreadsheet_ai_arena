# 📊 Spreadsheet Arena

**Spreadsheet Arena** is a benchmarking framework for evaluating AI agents and pipelines on spreadsheet-based tasks.

It provides a standardized way to run tasks, collect results, and compare performance across models, agents, and configurations.s

---

Spreadsheet Arena provides built-in commands to **fetch benchmark datasets** from the central repository.

Install local package (`arena`)

`sheet_arena`????

```bash
uv pip install -e .
```

```bash
arena checkout --list
```

```bash
arena checkout mimotable
```

```bash
arena run --dataset mimotable --sample 5
```