"Taxocontrol extract-transform-load script"
This project loads data via API, transforms it to proper state and stores it into folders and (Google Drive - now disabled).

It works in manual and scheduled modes.
For manual load some date - run the manual_load.py and input that date (or a date interval).
For scheduled load - run the scheduled_load.py. Loading occurs every day at 9:30 AM.

Project structure:

CONSTANTS.py (not included in repo by the reason of privacy) - contains API URL tail, login, password, and some lists for geo-mapping.

dataframe_transformations.py - contains all functions for work with Pandas DataFrames.

TK_utils.py - contains all other functions.

load_options_table.py - loads fresh ride options table for research and debug purposes.

manual_load.py - was mentioned above.

renaming_dicts.py (not included in repo by the reason of privacy) - contains Python dictionaries for renaming columns and categorical variables.

scheduled_load.py - was mentioned above.

bot_functions - couple of functions for telegram bot