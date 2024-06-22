from flask import Flask, render_template, request
from SQL.queries import *
from decimal import Decimal

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/dive')
def dive():
    table_names = query_get_table_names(Server1)
    max_id = max(query_get_last_id_value(Server1, table) for table in table_names)
    data = {}
    for table in table_names:
        data[table] = query_get_data_by_id(Server1, table, max_id)

    return render_template('dive.html', data=data, max_id=max_id)


@app.route('/all_dives')
def all_dives():
    table_names = query_get_table_names(Server1)
    data = {table: query_get_data_from_table(Server1, table) for table in table_names}
    return render_template('all_dives.html', data=data)


@app.route('/update_last_dive/<table>', methods=['POST'])
def update_last_dive(table):
    id_ = query_get_last_id_value(Server1, table)
    if id_ is None:
        return 'No data to update'

    # Extract form data
    data = {}
    for column in request.form:
        data[column] = request.form[column]

    query_update_row(Server1, table, id_, **data)

    return render_template('all_dives.html')


@app.route('/combat')
def data_option1():
    data = query_get_data_from_table(Server1, 'combat')
    columns = data[0]
    rows = data[1:]

    # Convert Decimal values to floats
    rows = [[float(cell) if isinstance(cell, Decimal) else cell for cell in row] for row in rows]

    # print(f"Columns: {columns}")  # Debugging: print columns
    # print(f"Rows: {rows}")  # Debugging: print rows

    return render_template('data/combat.html', columns=columns, rows=rows)


@app.route('/currency_gained')
def data_option2():
    data = query_get_data_from_table(Server1, 'currency_gained')
    columns = data[0]
    rows = data[1:]
    # Convert Decimal values to floats if necessary
    rows = [[float(cell) if isinstance(cell, Decimal) else cell for cell in row] for row in rows]
    # print(f"Columns: {columns}\nRows: {rows}")
    return render_template('data/currency_gained.html', columns=columns, rows=rows)


@app.route('/objectives_completed')
def data_option3():
    data = query_get_data_from_table(Server1, 'objectives_completed')
    columns = data[0]
    rows = data[1:]
    # Convert Decimal values to floats if necessary
    rows = [[float(cell) if isinstance(cell, Decimal) else cell for cell in row] for row in rows]
    # print(f"Columns: {columns}\nRows: {rows}")
    return render_template('data/objectives_completed.html', columns=columns, rows=rows)


@app.route('/samples_gained')
def data_option4():
    data = query_get_data_from_table(Server1, 'samples_gained')
    columns = data[0]
    rows = data[1:]
    # Convert Decimal values to floats if necessary
    rows = [[float(cell) if isinstance(cell, Decimal) else cell for cell in row] for row in rows]
    # print(f"Columns: {columns}\nRows: {rows}")
    return render_template('data/samples_gained.html', columns=columns, rows=rows)


@app.route('/input_combat')
def data_option5():
    return render_template('inputs/input_combat.html')


@app.route('/input_currency_gained')
def data_option6():
    return render_template('inputs/input_currency_gained.html')


@app.route('/input_objectives_completed')
def data_option7():
    return render_template('inputs/input_objectives_completed.html')


@app.route('/input_samples_gained')
def data_option8():
    return render_template('inputs/input_samples_gained.html')


@app.route('/submit_data_combat', methods=['POST'])
def submit_data_combat():
    id_ = query_get_last_id_value(Server1, 'combat')
    if id_ is None:
        id_ = 1
    else:
        id_ = int(id_) + 1

    # Extract form data
    data = {
        'kills': int(request.form['kills']),
        'accuracy': float(request.form['accuracy']),
        'shots_fired': int(request.form['shots_fired']),
        'deaths': int(request.form['deaths']),
        'stims_used': int(request.form['stims_used']),
        'accidentals': int(request.form['accidentals']),
        'samples_extracted': int(request.form['samples_extracted']),
        'stratagems_used': int(request.form['stratagems_used']),
        'melee_kills': int(request.form['melee_kills']),
        'times_reinforcing_': int(request.form['times_reinforcing_']),
        'friendly_fire_damage': int(request.form['friendly_fire_damage']),
        'distance_travelled': int(request.form['distance_travelled']),
    }

    query_put_row(Server1, 'combat', id=id_, **data)
    return render_template('submit_success.html')


@app.route('/submit_data_currency_gained', methods=['POST'])
def submit_data_currency_gained():
    id_ = query_get_last_id_value(Server1, 'currency_gained')
    if id_ is None:
        id_ = 1
    else:
        id_ = int(id_) + 1

    data = {
        'requisition': int(request.form['requisition']),
        'medals': int(request.form['medals']),
        'xp': int(request.form['xp'])
    }

    query_put_row(Server1, 'currency_gained', id=id_, **data)
    return render_template('submit_success.html')


@app.route('/submit_data_objectives_completed', methods=['POST'])
def submit_data_objectives_completed():
    id_ = query_get_last_id_value(Server1, 'objectives_completed')
    if id_ is None:
        id_ = 1
    else:
        id_ = int(id_) + 1

    data = {
        'main_objectives': int(request.form['main_objectives']),
        'optional_objectives': int(request.form['optional_objectives']),
        'helldivers_extracted': int(request.form['helldivers_extracted']),
        'outposts_destroyed_light': int(request.form['outposts_destroyed_light']),
        'outposts_destroyed_medium': int(request.form['outposts_destroyed_medium']),
        'outposts_destroyed_heavy': int(request.form['outposts_destroyed_heavy']),
        'mission_time_remaining': request.form['mission_time_remaining']
    }

    query_put_row(Server1, 'objectives_completed', id=id_, **data)
    return render_template('submit_success.html')


@app.route('/submit_data_samples_gained', methods=['POST'])
def submit_data_samples_gained():
    id_ = query_get_last_id_value(Server1, 'samples_gained')
    if id_ is None:
        id_ = 1
    else:
        id_ = int(id_) + 1

    data = {
        'green_samples': int(request.form['green_samples']),
        'orange_samples': int(request.form['orange_samples']),
        'violet_samples': int(request.form['violet_samples']),
    }

    query_put_row(Server1, 'samples_gained', id=id_, **data)
    return render_template('submit_success.html')


@app.route('/about')
def about():
    return render_template('about.html')


@app.route('/update_data_combat', methods=['POST'])
def update_data_combat():
    id_ = query_get_last_id_value(Server1, 'combat')
    if id_ is None:
        return 'No rows to update in combat data'

    data = {
        'kills': int(request.form['kills']),
        'accuracy': float(request.form['accuracy']),
        'shots_fired': int(request.form['shots_fired']),
        'deaths': int(request.form['deaths']),
        'stims_used': int(request.form['stims_used']),
        'accidentals': int(request.form['accidentals']),
        'samples_extracted': int(request.form['samples_extracted']),
        'stratagems_used': int(request.form['stratagems_used']),
        'melee_kills': int(request.form['melee_kills']),
        'times_reinforcing': int(request.form['times_reinforcing']),
        'friendly_fire_damage': int(request.form['friendly_fire_damage']),
        'distance_travelled': int(request.form['distance_travelled']),
    }

    query_update_row(Server1, 'combat', id_, data)
    return render_template('submit_success.html')


@app.route('/update_data_currency_gained', methods=['POST'])
def update_data_currency_gained():
    id_ = query_get_last_id_value(Server1, 'currency_gained')
    if id_ is None:
        return 'No rows to update in currency gained data'

    data = {
        'requisition': int(request.form['requisition']),
        'medals': int(request.form['medals']),
        'xp': int(request.form['xp'])
    }

    query_update_row(Server1, 'currency_gained', id_, data)
    return render_template('submit_success.html')


@app.route('/update_data_objectives_completed', methods=['POST'])
def update_data_objectives_completed():
    id_ = query_get_last_id_value(Server1, 'objectives_completed')
    if id_ is None:
        return 'No rows to update in objectives completed data'

    data = {
        'main_objectives': int(request.form['main_objectives']),
        'optional_objectives': int(request.form['optional_objectives']),
        'helldivers_extracted': int(request.form['helldivers_extracted']),
        'outposts_destroyed_light': int(request.form['outposts_destroyed_light']),
        'outposts_destroyed_medium': int(request.form['outposts_destroyed_medium']),
        'outposts_destroyed_heavy': int(request.form['outposts_destroyed_heavy']),
        'mission_time_remaining': request.form['mission_time_remaining']
    }

    query_update_row(Server1, 'objectives_completed', id_, data)
    return render_template('submit_success.html')


@app.route('/update_data_samples_gained', methods=['POST'])
def update_data_samples_gained():
    id_ = query_get_last_id_value(Server1, 'samples_gained')
    if id_ is None:
        return 'No rows to update in samples gained data'

    data = {
        'green_samples': int(request.form['green_samples']),
        'orange_samples': int(request.form['orange_samples']),
        'violet_samples': int(request.form['violet_samples']),
    }

    query_update_row(Server1, 'samples_gained', id_, data)
    return render_template('submit_success.html')


@app.route('/delete_last_row/<table_name>', methods=['POST'])
def delete_last_row(table_name):
    id_ = query_get_last_id_value(Server1, table_name)
    if id_ is None:
        return f'No rows to delete in {table_name} data'

    query_delete_row(Server1, table_name, id_)
    return render_template('submit_success.html')


@app.route('/delete_last_dive', methods=['POST'])
def delete_last_dive():
    # Get the maximum ID value across all tables
    max_id = max(query_get_last_id_value(Server1, table) for table in query_get_table_names(Server1))

    if max_id is not None:
        # Delete rows with the maximum ID value from all tables
        for table in query_get_table_names(Server1):
            query_delete_row(Server1, table, max_id)

    return render_template('submit_success_all_dives.html', redirect_url='/all_dives')


if __name__ == '__main__':
    app.run(debug=True)
