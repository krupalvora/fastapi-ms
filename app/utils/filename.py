# function to set filename for the report report name + datetime

import os
from datetime import datetime
def get_filename(report_name):
    return f"{report_name}_{datetime.now().strftime('%Y%m%d')}.csv"