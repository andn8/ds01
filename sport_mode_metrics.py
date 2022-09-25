import pandas_gbq
import numpy as np
from google.oauth2 import service_account
import gspread
from google.oauth2.service_account import Credentials
import utils
import datetime
import time


def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return str(len(str_list) + 1)


def ios_sport_mode_metric_sheet_1():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet_1 = f"""with active_users as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(distinct user_id) as active_users  from `analytics_214553440.events_*`,unnest(user_properties) as u where event_name='user_engagement' 
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, platform, mode_name),

    time_on_app as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, sum(p.value.int_value)/count(distinct user_id) / 1000 / 60 as time_on_app from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u where p.key='engagement_time_msec' and event_name = 'user_engagement'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, mode_name),

    screen_view as
    (SELECT event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as screen_view  from `analytics_214553440.events_*`, unnest(user_properties) as u where event_name='screen_view' 
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, platform, mode_name),

    read_article as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as read_article from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='read_all_article'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    AND p.key = 'item_topics'
    group by event_date, platform, mode_name),

    play_video as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as play_video from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='play_video'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    AND p.key = 'item_topics'
    group by event_date, platform, mode_name),

    open_push as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as open_push from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='open_push_notification'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    AND p.key = 'item_topics'
    group by event_date, platform, mode_name)

    select a.event_date, a.platform, a.mode_name, a.active_users, t.time_on_app, s.screen_view, r.read_article, p.play_video, o.open_push from active_users a
    inner join time_on_app t on t.event_date = a.event_date and t.mode_name = a.mode_name
    inner join screen_view s on s.event_date = a.event_date and s.mode_name = a.mode_name
    inner join read_article r on r.event_date = a.event_date and r.mode_name = a.mode_name
    inner join open_push o on o.event_date = a.event_date and o.mode_name = a.mode_name
    inner join play_video p on p.event_date = a.event_date and p.mode_name = a.mode_name"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet_1, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet1")

    next_row = next_available_row(worksheet)

    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", (cell_values[2]))
    worksheet.update(f"D{next_row}", int(cell_values[3]))
    worksheet.update(f"E{next_row}", float(cell_values[4]))
    worksheet.update(f"F{next_row}", float(cell_values[5]))
    worksheet.update(f"G{next_row}", float(cell_values[6]))
    worksheet.update(f"H{next_row}", float(cell_values[7]))
    worksheet.update(f"I{next_row}", float(cell_values[8]))

    next_row_1 = str(int(next_row) + 1)

    worksheet.update(f"A{next_row_1}", int(cell_values[9]))
    worksheet.update(f"B{next_row_1}", (cell_values[10]))
    worksheet.update(f"C{next_row_1}", (cell_values[11]))
    worksheet.update(f"D{next_row_1}", int(cell_values[12]))
    worksheet.update(f"E{next_row_1}", float(cell_values[13]))
    worksheet.update(f"F{next_row_1}", float(cell_values[14]))
    worksheet.update(f"G{next_row_1}", float(cell_values[15]))
    worksheet.update(f"H{next_row_1}", float(cell_values[16]))
    worksheet.update(f"I{next_row_1}", float(cell_values[17]))

    return cell_values


def ios_sport_mode_metric_sheet_2():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet = f"""(select event_date, 'ios' as platform, mode_name,
       (select p.value.string_value from UNNEST(event_params) AS p where p.key='item_type') as type,
       sum((select CAST(TRIM(p.value.string_value) AS NUMERIC) from UNNEST(event_params) AS p where p.key='duration'))/count(distinct user_id)/60 as duration
        from (select event_date, user_id, u.value.string_value as mode_name, event_params
        from `analytics_214553440.events_*`, unnest(user_properties) as u WHERE event_name = 'view_content' 
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND u.key='u_mode' AND u.value.string_value = 'sport')
        group by 1,2,3,4
    )"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet2")

    next_row = next_available_row(worksheet)
    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", (cell_values[2]))
    worksheet.update(f"D{next_row}", (cell_values[3].replace("author", "article")))
    worksheet.update(f"E{next_row}", float(cell_values[4]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[5]))
    worksheet.update(f"B{next_row}", (cell_values[6]))
    worksheet.update(f"C{next_row}", (cell_values[7]))
    worksheet.update(f"D{next_row}", (cell_values[8]))
    worksheet.update(f"E{next_row}", float(cell_values[9]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[10]))
    worksheet.update(f"B{next_row}", (cell_values[11]))
    worksheet.update(f"C{next_row}", (cell_values[12]))
    worksheet.update(f"D{next_row}", (cell_values[13]))
    worksheet.update(f"E{next_row}", float(cell_values[14]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[15]))
    worksheet.update(f"B{next_row}", (cell_values[16]))
    worksheet.update(f"C{next_row}", (cell_values[17]))
    worksheet.update(f"D{next_row}", (cell_values[18]))
    worksheet.update(f"E{next_row}", float(cell_values[19]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[20]))
    worksheet.update(f"B{next_row}", (cell_values[21]))
    worksheet.update(f"C{next_row}", (cell_values[22]))
    worksheet.update(f"D{next_row}", (cell_values[23]))
    worksheet.update(f"E{next_row}", float(cell_values[24]))

    return cell_values


def ios_sport_mode_metric_sheet_5():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet = f"""select event_date, 'ios' as platform, p.value.string_value as mode_name, count(distinct user_id) as count_selection
    from `analytics_214553440.events_*`, unnest(event_params) as p WHERE event_name = 'select_mode' AND p.key = "item_name"
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    group by 1,2,3"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet5")

    next_row = next_available_row(worksheet)
    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", (cell_values[2]))
    worksheet.update(f"D{next_row}", int(cell_values[3]))

    next_row_1 = str(int(next_row) + 1)

    worksheet.update(f"A{next_row_1}", int(cell_values[4]))
    worksheet.update(f"B{next_row_1}", (cell_values[5]))
    worksheet.update(f"C{next_row_1}", (cell_values[6]))
    worksheet.update(f"D{next_row_1}", int(cell_values[7]))

    return cell_values


def ios_sport_mode_metric_sheet_6():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )
    list_fanclub = [
        "Manchester United",
        "Chelsea",
        "Paris Saint Germain",
        "Barcelona",
        "Manchester City",
        "Liverpool",
        "Real Madrid",
        "Juventus",
        "Arsenal",
        "Bayern Munich",
        "Tottenham Hotspur",
    ]
    for data in list_fanclub:
        sql_sheet = f"""with metric_club as
        (with id_tracking as
        (select idfa as user_id from `tinmoi24.fan_club` where team_name = '{data}'),

        t as
        (select event_date, (sum(p.value.int_value) / 1000 / 60)/count(distinct user_id) as time_on_app from `analytics_214553440.events_*`,unnest(event_params) as p where p.key='engagement_time_msec' and event_name='user_engagement'
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1),

        s as
        (select event_date, count(event_name)/count(distinct user_id) as screenview from `analytics_214553440.events_*` WHERE event_name='screen_view'
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1),

        r as
        (select event_date, count(p.value.string_value)/count(distinct user_id) as read_listing from `analytics_214553440.events_*`,UNNEST(event_params) AS p WHERE event_name='read_all_article' AND p.key='location' AND p.value.string_value = "read_listing_article"
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1),

        e as
        (select event_date, count(p.value.string_value)/count(distinct user_id) as read_push from `analytics_214553440.events_*`,UNNEST(event_params) AS p WHERE event_name='read_all_article' AND p.key='location' AND p.value.string_value = "read_push_article"
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1),

        f as
        (select event_date, count(p.value.string_value)/count(distinct user_id) as read_relative from `analytics_214553440.events_*`,UNNEST(event_params) AS p WHERE event_name='read_all_article' AND p.key='location' AND (p.value.string_value = "read_relative_article" or p.value.string_value = "read_popular_article")
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1),

        u as
        (select event_date, count(distinct user_id) as DAU from `analytics_214553440.events_*` where event_name='user_engagement'
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        and user_id in (select user_id from id_tracking)
        group by 1)

        select u.event_date, DAU, read_listing, read_push, read_relative, screenview, time_on_app
        from u inner join r using(event_date)
        inner join e using(event_date)
        inner join f using(event_date)
        inner join s using(event_date)
        inner join t using(event_date)
        group by 1,2,3,4,5,6,7
        order by 1),

        club_info as
        (select FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as event_date, 'ios' as platform, team_name, count(idfa) as total_mem from `tinmoi24.fan_club` where team_name = '{data}' group by 1,2,3)

        select * from club_info
        inner join metric_club using(event_date)"""

        dau_new_table = pandas_gbq.read_gbq(
            sql_sheet, project_id="tin-hay-24h", credentials=credentials
        )

        cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
        update_cells = [(i) for i in cell_values]

        gc = gspread.authorize(creds)

        worksheet = gc.open_by_key(
            "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
        ).worksheet("Sheet6")

        next_row = next_available_row(worksheet)
        worksheet.update(f"A{next_row}", int(cell_values[0]))
        worksheet.update(f"B{next_row}", (cell_values[1]))
        worksheet.update(f"C{next_row}", (cell_values[2]))
        worksheet.update(f"D{next_row}", int(cell_values[3]))
        worksheet.update(f"E{next_row}", float(cell_values[4]))
        worksheet.update(f"F{next_row}", float(cell_values[5]))
        worksheet.update(f"G{next_row}", float(cell_values[6]))
        worksheet.update(f"H{next_row}", float(cell_values[7]))
        worksheet.update(f"I{next_row}", float(cell_values[8]))
        worksheet.update(f"J{next_row}", float(cell_values[9]))

    return f"Done sheet 6!!!"


def ios_sport_mode_metric_sheet_8():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet = f"""(select event_date, 'ios' as platform, mode_name,
       (select p.value.string_value from UNNEST(event_params) AS p where p.key='item_type') as type,
       sum((select count(p.value.string_value) from UNNEST(event_params) AS p where p.key='item_type')) as count
        from (select event_date, user_id, u.value.string_value as mode_name, event_params
        from `analytics_214553440.events_*`, unnest(user_properties) as u WHERE event_name = 'view_content' 
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND u.key='u_mode' AND u.value.string_value = 'sport')
        group by 1,2,3,4)"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet8")

    next_row = next_available_row(worksheet)
    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", (cell_values[2]))
    worksheet.update(f"D{next_row}", (cell_values[3].replace("author", "article")))
    worksheet.update(f"E{next_row}", float(cell_values[4]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[5]))
    worksheet.update(f"B{next_row}", (cell_values[6]))
    worksheet.update(f"C{next_row}", (cell_values[7]))
    worksheet.update(f"D{next_row}", (cell_values[8]))
    worksheet.update(f"E{next_row}", float(cell_values[9]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[10]))
    worksheet.update(f"B{next_row}", (cell_values[11]))
    worksheet.update(f"C{next_row}", (cell_values[12]))
    worksheet.update(f"D{next_row}", (cell_values[13]))
    worksheet.update(f"E{next_row}", float(cell_values[14]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[15]))
    worksheet.update(f"B{next_row}", (cell_values[16]))
    worksheet.update(f"C{next_row}", (cell_values[17]))
    worksheet.update(f"D{next_row}", (cell_values[18]))
    worksheet.update(f"E{next_row}", float(cell_values[19]))

    next_row = str(int(next_row) + 1)

    worksheet.update(f"A{next_row}", int(cell_values[20]))
    worksheet.update(f"B{next_row}", (cell_values[21]))
    worksheet.update(f"C{next_row}", (cell_values[22]))
    worksheet.update(f"D{next_row}", (cell_values[23]))
    worksheet.update(f"E{next_row}", float(cell_values[24]))

    return cell_values


def ios_sport_mode_metric_sheet_9():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet = f"""with active_users as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(distinct user_id) as active_users  from `analytics_214553440.events_*`,unnest(user_properties) as u where event_name='user_engagement' 
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, platform, mode_name),

    time_on_app as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, sum(p.value.int_value)/count(distinct user_id) / 1000 / 60 as time_on_app from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u where p.key='engagement_time_msec' and event_name = 'user_engagement'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, mode_name),

    screen_view as
    (SELECT event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as screen_view  from `analytics_214553440.events_*`, unnest(user_properties) as u where event_name='screen_view' 
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    group by event_date, platform, mode_name),

    read_article as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as read_article from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='read_all_article'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    AND p.key = 'item_topics'
    group by event_date, platform, mode_name),

    play_video as
    (select event_date, 'ios' as platform, u.value.string_value as mode_name, count(event_name)/count(distinct user_id) as play_video from `analytics_214553440.events_*`,unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='play_video'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key='u_mode' AND (u.value.string_value = 'sport' OR u.value.string_value = 'normal')
    AND p.key = 'item_topics'
    group by event_date, platform, mode_name)
    select a.event_date, a.platform, a.active_users, t.time_on_app, s.screen_view, r.read_article, p.play_video from active_users a
    inner join time_on_app t on t.event_date = a.event_date and t.mode_name = a.mode_name
    inner join screen_view s on s.event_date = a.event_date and s.mode_name = a.mode_name
    inner join read_article r on r.event_date = a.event_date and r.mode_name = a.mode_name
    inner join play_video p on p.event_date = a.event_date and p.mode_name = a.mode_name
    where a.mode_name = 'sport'"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet9")

    next_row = next_available_row(worksheet)
    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", int(cell_values[2]))
    worksheet.update(f"D{next_row}", float(cell_values[3]))
    worksheet.update(f"E{next_row}", float(cell_values[4]))
    worksheet.update(f"F{next_row}", float(cell_values[5]))
    worksheet.update(f"G{next_row}", float(cell_values[6]))

    return cell_values


def ios_sport_mode_metric_sheet_10():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_SOCCER_EXCEL"]}""",
        scopes=scopes,
    )
    credentials = service_account.Credentials.from_service_account_file(
        f"""{utils.Config["ROOT_PATH"]}/{utils.Config["SERVICE_ACCOUNT_IOS"]}"""
    )

    sql_sheet = f"""with first_open as
    (select distinct user_pseudo_id as first_open_user from `analytics_214553440.events_*`, unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='first_open'
    AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    ),

    first_open_sport as
    (select distinct user_pseudo_id as day_0_user_sport from `analytics_214553440.events_*`, unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='user_engagement'
    AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    AND u.key = 'u_mode' AND u.value.string_value = 'sport'
    and user_pseudo_id in (select first_open_user from first_open)
    ),

    first_open_normal as
    (select distinct user_pseudo_id as day_0_user_normal from `analytics_214553440.events_*`, unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='user_engagement'
    AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    AND u.key = 'u_mode' AND u.value.string_value = 'normal'
    and user_pseudo_id in (select first_open_user from first_open)
    ),

    day_0_sport as
    (select FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as event_date, count(distinct day_0_user_sport) as day_0_user_sport from first_open_sport),

    day_0_normal as
    (select FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as event_date, count(distinct day_0_user_normal) as day_0_user_normal from first_open_normal),

    day_1_sport as
    (select event_date, u.value.string_value as mode_name, count(distinct user_pseudo_id) as day_1_user_sport from `analytics_214553440.events_*`, unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='user_engagement'
    AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key = 'u_mode' AND u.value.string_value = 'sport'
    and user_pseudo_id in (select day_0_user_sport from first_open_sport)
    group by event_date, mode_name),

    day_1_normal as
    (select event_date, u.value.string_value as mode_name, count(distinct user_pseudo_id) as day_1_user_normal from `analytics_214553440.events_*`, unnest(event_params) as p, unnest(user_properties) as u WHERE event_name='user_engagement'
    AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND u.key = 'u_mode' AND u.value.string_value = 'normal'
    and user_pseudo_id in (select day_0_user_normal from first_open_normal)
    group by event_date, mode_name)

    select s.event_date,'ios' as platform, day_1_user_sport/day_0_user_sport*100 as rr_1_sport, day_1_user_normal/day_0_user_normal*100 as rr_1_normal from day_0_sport s
    inner join day_0_normal n on s.event_date = n.event_date
    inner join day_1_sport p on s.event_date = p.event_date
    inner join day_1_normal q on s.event_date = q.event_date"""

    dau_new_table = pandas_gbq.read_gbq(
        sql_sheet, project_id="tin-hay-24h", credentials=credentials
    )

    cell_values = np.array(dau_new_table.values.tolist()).flatten().tolist()
    update_cells = [(i) for i in cell_values]

    gc = gspread.authorize(creds)

    worksheet = gc.open_by_key(
        "1-aG66iyL1RowdhzcCk1-gPCZ33qXunLbu14GNtYnU6U"
    ).worksheet("Sheet10")

    next_row = next_available_row(worksheet)
    worksheet.update(f"A{next_row}", int(cell_values[0]))
    worksheet.update(f"B{next_row}", (cell_values[1]))
    worksheet.update(f"C{next_row}", float(cell_values[2]))
    worksheet.update(f"D{next_row}", float(cell_values[3]))

    return cell_values


t = 0
# status_sheet_1 = ios_sport_mode_metric_sheet_1()
# time.sleep(t)

# status_sheet_2 = ios_sport_mode_metric_sheet_2()

# time.sleep(t)
# status_sheet_5 = ios_sport_mode_metric_sheet_5()
# time.sleep(t)
# status_sheet_6 = ios_sport_mode_metric_sheet_6()

# time.sleep(t)
status_sheet_8 = ios_sport_mode_metric_sheet_8()

time.sleep(t)
status_sheet_9 = ios_sport_mode_metric_sheet_9()

time.sleep(t)

status_sheet_10 = ios_sport_mode_metric_sheet_10()
