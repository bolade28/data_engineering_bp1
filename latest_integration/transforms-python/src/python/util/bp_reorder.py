# ===== import: python function
import re

# ===== import: palantir functions

# ===== import: our functions


def schema_reader(schema):
    lines = []

    for line in schema:
        all = re.findall('\s*(StructField\()(\w+)', str(line))
        if all and all[0]:
            lines.append(all[0][1])

    return lines
