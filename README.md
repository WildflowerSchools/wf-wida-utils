# wf_base_data

Python tools for working with Wildflower Schools base data

## Tasks
* Use `__getitem__` so that user can pull individual data tables without referencing internals
* Define generic `WildflowerDatabase` and make `WildflowerDatabasePandas` a subclass
* Move methods for pulling and adding student records to parent `WildflowerDatabase` class (so they can be used by other database implementations)
* Generalize student ID generation to other objects (teachers, schools, hubs, etc.)
