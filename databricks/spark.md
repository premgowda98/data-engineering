## Apache Spark & Databricks: Quick Notes

### What is Spark?
Apache Spark is an open-source, distributed computing system for big data processing and analytics. It is fast, scalable, and supports SQL, streaming, machine learning, and graph processing.

### Spark Architecture
**Key Components:**
- **Driver:** Coordinates the application, translates user code into tasks.
- **Cluster Manager:** Allocates resources (e.g., YARN, Mesos, Kubernetes, Standalone).
- **Executors:** Run tasks and store data.

**Diagram:**

```
		  +-------------------+
		  |     Driver        |
		  +-------------------+
						|
		  +-------------------+
		  | Cluster Manager   |
		  +-------------------+
			/        |        \
	+--------+ +--------+ +--------+
	|Executor| |Executor| |Executor|
	+--------+ +--------+ +--------+
```

### What is Databricks?
Databricks is a cloud-based platform built on top of Apache Spark. It provides collaborative notebooks, managed Spark clusters, and tools for data engineering, machine learning, and analytics.

**Relation:**
- Databricks simplifies using Spark by handling cluster setup, scaling, and management.
- It adds features like collaborative notebooks, job scheduling, and integrations with cloud storage.

**Diagram:**

```
	+-------------------+
	|   Databricks UI   |
	+-------------------+
				|
	+-------------------+
	| Managed Spark     |
	|   Clusters        |
	+-------------------+
				|
	+-------------------+
	| Cloud Storage     |
	+-------------------+
```

---
...existing code...
