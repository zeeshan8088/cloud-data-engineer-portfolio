import re
path = r'c:\Users\Home\cloud-data-engineer-portfolio\projects\week10-11-capstone\dbt\models\marts\_marts_models.yml'
with open(path, 'r') as f:
    lines = f.readlines()

new_lines = []
for idx, line in enumerate(lines):
    new_lines.append(line)
    if 'dbt_utils.accepted_range:' in line:
        indent = len(line) - len(line.lstrip())
        new_lines.append(' ' * (indent + 4) + 'arguments:\n')
        # next two lines should be indented by 2 spaces more, but dbt is fine if they are at the same level or more under arguments
        # Actually let's just insert 'arguments:' and let the next lines follow since they are already indented by 2 more spaces

# wait, we need to indent the subsequent min_value, max_value, inclusive
# let's just do it properly
with open(path, 'r') as f:
    text = f.read()

# Replace all occurrences
text = re.sub(
    r'(dbt_utils\.accepted_range:)\n(\s+)(min_value:)',
    r'\1\n\2arguments:\n\2  \3',
    text
)
# Since inclusive also needs to be indented or just leave it... wait, YAML doesn't care if arguments' children are 2 spaces or 4.
# Let's write another regex to indent anything under accepted_range until a new test or column or empty line.

# Simpler: just replace 'dbt_utils.accepted_range:' with something that will make the subsequent lines be part of arguments
# Actually, since it's just min_value, max_value, and inclusive, let's fix it by adding 2 spaces to those lines if they follow accepted_range

with open(path, 'w') as f:
    f.write(text)

# Let me run a simple re.sub to fix the inclusive and max_value indentations as well
for k in ['max_value', 'inclusive']:
    with open(path, 'r') as f:
        t = f.read()
    t = re.sub(r'\n(\s+)(' + k + r':)', r'\n\1  \2', t)
    with open(path, 'w') as f:
        f.write(t)
