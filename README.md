# wf_base_data

Python tools for working with Wildflower Schools base data

## Tasks
* Fix up data types on data tables (e.g., Wildflower student IDs are objects)
* Consider rearranging constructors for inherited classes
* Use `__getitem__` so that user can pull individual data tables without referencing internals
* Define generic `WildflowerDatabase` and make `WildflowerDatabasePandas` a subclass
* Move methods for pulling and adding student records to parent `WildflowerDatabase` class (so they can be used by other database implementations)
* Add dupe checking to student ID generation method
* Generalize student ID generation to other objects (teachers, schools, hubs, etc.)
