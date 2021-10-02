# wf_core_data

Python tools for working with Wildflower Schools core data

## Tasks
* NWEA: Clean up results parsing functions
* NWEA: Retool results parsing to produce three tables: test events, student info (information that is usually constant across schools and years), and student assignments (school, grade, and classroom assignments for each year/term)
* NWEA: Turn subject and course into categorical variables to control ordering
* NWEA: Add function that reports number of test events by category
* NWEA: Retool joining of student-level summary with student info and or student assignments to be more flexible about time grouping
* NWEA: Make group analysis more flexible to time grouping of earlier student stage
* Update naming of submodules and functions to be more consistent across assessments
* Propagate changes above to FastBridge analysis
