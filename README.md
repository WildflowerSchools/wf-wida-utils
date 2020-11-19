# wf_base_data

Python tools for working with Wildflower Schools base data

## Tasks
* Figure out why `student_last_day` ends up being a mix of `NaT` and `NaN` for missing values
* Add methods to pull all current Wildflower student IDs, all TC student IDs in history, all TC student IDs in mapping
* Add school ID when pulling student data for multiple schools from Transparent Classroom
* Add pull datetime when pulling student data from TC and writing to database
* Add method for pulling student data directly from TC and writing to database
* Make database structure an OrderedDict so user can specify order of data tables (e.g., for saving to Google sheets)
* Add method for writing database to Google Sheets
* Add method for writing database to local file(s)
* Add tables for schools, classrooms, teachers
* Generalize student ID generation to other objects (teachers, schools, hubs, etc.)
* Use `__getitem__` so that user can pull individual data tables without referencing internals
* Define generic `WildflowerDatabase` and make `WildflowerDatabasePandas` a subclass
* Move methods for pulling and adding student records to parent `WildflowerDatabase` class (so they can be used by other database implementations)
