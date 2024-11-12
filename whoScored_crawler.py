import math
import time
from typing import OrderedDict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
# built in this way: { minute: [home team possession, away team possession], ... }
time_dict = {}


def insert_window_possession(current_minute, driver, is_first_half, is_over_time):
    """

    :param is_first_half: is it first half (including overtime first half)
    :param is_over_time: is it over time
    :param current_minute: the current minute
    :param driver: web drive
    """
    # Locate the home data using full XPath
    home_element = driver.find_element(By.XPATH,
                                       '/html/body/div[3]/div[5]/div[2]/div[2]/div[1]/ul[1]/li[3]/div[1]/span[1]')
    home_value = home_element.text  # Get the text (value inside the element)
    time.sleep(0.5)
    # Locate the away data using full XPath
    away_element = driver.find_element(By.XPATH,
                                       '/html/body/div[3]/div[5]/div[2]/div[2]/div[1]/ul[1]/li[3]/div[1]/span[3]')
    away_value = away_element.text  # Get the text (value inside the element)
    time.sleep(0.5)

    data_values = [float(home_value), float(away_value)]
    # insert the time to the dictionary
    current_half_time = half_time(is_first_half, is_over_time)
    if current_half_time < current_minute:

        current_time = str(current_half_time) + "+" + str(current_minute % current_half_time)
    else:
        current_time = str(current_minute)
    time_dict[current_time] = data_values


def calc_half_iterations(window_time, half_minutes, is_first_half, is_overtime):
    # window_time = -1 * window_time
    iter = 0
    if not is_overtime:
        if is_first_half:
            for i in range(window_time, half_minutes+1, window_time):
                iter += 1
        else:
            for i in range(math.ceil(45 / window_time) * window_time, half_minutes+1, window_time):
                iter += 1
    else:
        if is_first_half:
            for i in range(half_minutes, 90, window_time):
                iter += 1
        else:
            for i in range(half_minutes, 105, window_time):
                iter += 1
    return iter


def move_window_left(number_of_steps, actions):
    """

    :param number_of_steps: number of minutes according to the time window.
    :param actions: gives access to the driver
    """
    for i in range(number_of_steps):
        actions.send_keys(Keys.ARROW_LEFT).perform()
        time.sleep(0.2)


def calculate_half_last_window(half_time_minutes, window_time, actions, driver, is_first_half, is_over_time):
    """
    :param is_first_half: is it first half (including overtime first half)
    :param is_over_time: is it over time
    :param half_time_minutes: number of minutes in the half
    :param window_time: the length of the window in minutes
    :param actions: element for preforming actions on the driver
    :param driver: webdriver
    :return: the length of the last window in this half because it's different than the regular time window.
    """
    insert_window_possession(half_time_minutes, driver, is_first_half, is_over_time)
    move_window_left(half_time_minutes % window_time, actions)
    return half_time_minutes % window_time


def get_current_half_minutes(driver, time_offset):
    second_tooltip_text = driver.execute_script("return document.querySelectorAll('.noUi-tooltip')[1].textContent;")
    # Add a small wait after interacting with the slider
    time.sleep(1)
    # Attempt to access tooltip text directly via JavaScript
    if '+' in second_tooltip_text:
        minutes = second_tooltip_text.split('+')
        return int(minutes[0]) + int(minutes[1]) - time_offset
    return int(second_tooltip_text) - time_offset


def access_to_time_slider(actions, driver):
    """
    take the keyboard to the right end of the time slider
    :param actions: element for preforming actions on the driver
    :param driver: webdriver
    """

    element = driver.find_element(By.XPATH, '/html/body/div[3]/div[5]/div[2]/div[3]/div[1]/div[2]/div/div[3]/div')
    time.sleep(0.5)
    actions.move_to_element(element).click().perform()
    actions.move_to_element(element).click().perform()
    actions.move_to_element(element).click().perform()


def half_time(is_first_half, is_over_time):
    """

    :param is_first_half: is it first half (including overtime first half)
    :param is_over_time: is it over time
    :return: how many minutes passed until the end of that half time
    """
    if is_over_time:
        if is_first_half:
            return 105
        return 120

    elif not is_first_half:
        return 90
    return 45


def time_offset_calculate(is_first_half, is_over_time):
    """
    calculate the offset of the current half because the half time is usualy 15-20 / 45-55 minutes.
    :param is_first_half: is it first half (including overtime first half)
    :param is_over_time: is it over time
    :return: the offset in minutes
    """
    if is_over_time:
        if is_first_half:
            return 90
        return 105

    elif not is_first_half:
        return 45
    return 0


def half_possessions(window_time, half_time, actions, driver, is_first_half=False, is_over_time=False):
    last_window = 0
    time_offset = time_offset_calculate(is_first_half, is_over_time)
    # if time window bigger than overtime, we collect the whole half period.
    if is_over_time and window_time >= half_time:
        insert_window_possession(half_time + time_offset, driver, is_first_half, is_over_time)
        move_window_left(half_time, actions)
        return
    # if there is one window that should be shorter than the regular one we do it here
    if (half_time + time_offset) % window_time != 0:
        last_window = calculate_half_last_window(half_time + time_offset, window_time, actions, driver, is_first_half, is_over_time)
    number_of_iterations = calc_half_iterations(window_time, time_offset + get_current_half_minutes(driver, time_offset)
                                                , is_first_half, is_over_time)

    # in case of only one window when further calculations don't needed
    if number_of_iterations == 1:
        insert_window_possession(half_time + time_offset, driver, is_first_half, is_over_time)
        move_window_left(window_time, actions)
        return
    # calculating the last movement to the previous half
    last_movement = half_time - (number_of_iterations-1) * window_time - last_window + 1

    for i in range(number_of_iterations):
        insert_window_possession(half_time + time_offset - last_window - i * window_time, driver, is_first_half, is_over_time)
        if i == number_of_iterations - 1 and is_first_half and not is_over_time:
            return
        elif i == number_of_iterations - 1:
            move_window_left(last_movement, actions)
        else:
            move_window_left(window_time, actions)
    # moving to the end of the previous half


def check_if_url_is_bad(driver, is_overtime, actions):
    if is_overtime:
        return 150 < get_current_half_minutes(driver, time_offset_calculate(False, True))

    else:
        return 120 < get_current_half_minutes(driver, time_offset_calculate(False, False))


def is_over_time(driver):
    second_tooltip_text = driver.execute_script("return document.querySelectorAll('.noUi-tooltip')[1].textContent;")
    # Add a small wait after interacting with the slider
    time.sleep(1)
    # Attempt to access tooltip text directly via JavaScript
    if '+' in second_tooltip_text:
        minutes = second_tooltip_text.split('+')[0]
    else:
        minutes = second_tooltip_text
    return minutes == "120"


def bad_time_dict(time_window):
    # Locate the home data using full XPath
    home_element = driver.find_element(By.XPATH,
                                       '/html/body/div[3]/div[5]/div[2]/div[2]/div[1]/ul[1]/li[3]/div[1]/span[1]')
    home_value = home_element.text  # Get the text (value inside the element)
    time.sleep(0.5)
    # Locate the away data using full XPath
    away_element = driver.find_element(By.XPATH,
                                       '/html/body/div[3]/div[5]/div[2]/div[2]/div[1]/ul[1]/li[3]/div[1]/span[3]')
    away_value = away_element.text  # Get the text (value inside the element)
    time.sleep(0.5)

    data_values = [float(home_value), float(away_value)]
    # insert the time to the dictionary
    bad_time = {}
    for i in range(0, 90, time_window):
        bad_time[str(i)] = data_values
    bad_time = dict(reversed(list(bad_time.items())))
    return bad_time


def get_possession(window_time, game_id, driver, overtime_first_half=0, overtime_second_half=0):
    """
    the main function that activates all the other functions.
    :param window_time: length of the time window in minutes
    :param game_id: string of the game id
    :param overtime_first_half: length of the overtime first half in minutes
    :param overtime_second_half: length of the overtime second half in minutes
    """
    if window_time >= 45:
        print("screw you, I'm not doing that window time")
        return {}

    # Open the target URL
    driver.get("https://www.whoscored.com/Matches/" + game_id + "/Live")

    # Wait for the page to load (increase time if necessary)
    time.sleep(3)


    # create_monthly_jsons(game_id, driver, stage_id, date)

    try:
        actions = ActionChains(driver)
        access_to_time_slider(actions, driver)
        time.sleep(0.35)
        is_overtime = is_over_time(driver)
        time.sleep(0.3)
        # some games don't have valid data, checking if it's the situation ant return a default values
        # of the last minute possession
        if check_if_url_is_bad(driver, is_overtime, actions):
            return bad_time_dict(window_time)
        # overtime second half
        if is_overtime:
            overtime_second_half = get_current_half_minutes(driver, time_offset_calculate(False, True))
            time.sleep(0.6)
            half_possessions(window_time, overtime_second_half, actions, driver, False, True)
            # overtime first half
            time.sleep(0.5)
            overtime_first_half = get_current_half_minutes(driver, time_offset_calculate(True, True))
            time.sleep(0.5)
            half_possessions(window_time, overtime_first_half, actions, driver, True, True)
            time.sleep(0.5)
        # second half
        second_half_minutes = get_current_half_minutes(driver, time_offset_calculate(False, False))
        time.sleep(0.35)
        half_possessions(window_time, second_half_minutes, actions, driver)
        # first half
        time.sleep(0.35)
        first_half_minutes = get_current_half_minutes(driver, time_offset_calculate(True, False))
        time.sleep(0.35)
        half_possessions(window_time, first_half_minutes, actions, driver, True)
        if '0' in time_dict:
            del time_dict['0']
        return time_dict

    except Exception as e:
        print(f"An error occurred: {e}")


def get_current_game_ids(finished_game_ids_json_path, new_games_id):

    with open(finished_game_ids_json_path, 'r') as f:
        finished_game_ids = json.load(f)

def process_game(game_id, window_time):
    # Use the Selenium driver to fetch possession data for the given game_id
    options = Options()
    options.add_argument("--start-maximized")  # Optional: Start the browser maximized
    driver = webdriver.Chrome(options=options)

    possessions_dict = get_possession(window_time, game_id, driver)
    driver.quit()
    return {game_id: possessions_dict}


def main():
    # Define paths for your files
    new_games_ids_path = r'C:\Users\user\Desktop\jsons\new_games_id.json'
    possessions_json = r'C:\Users\user\Desktop\jsons\new_possessions_data.json'

    # Load game_ids from the JSON file
    with open(new_games_ids_path, 'r') as file:
        game_ids = json.load(file)

    # Load any existing possession data, if the JSON file already exists
    if Path(possessions_json).exists():
        with open(possessions_json, 'r') as file:
            all_possessions_data = json.load(file)
    else:
        all_possessions_data = {}

    # Set the window time for possession calculation (e.g., 15 minutes)
    window_time = 10  # Update to your actual desired window time
    save_interval = 5  # Number of processed games before saving to JSON
    processed_count = 0  # Counter to keep track of processed games

    # Use ProcessPoolExecutor to process multiple games in parallel
    with ProcessPoolExecutor() as executor:
        # Submit each game_id to be processed by `process_game` function in parallel
        futures = {executor.submit(process_game, game_id, window_time): game_id for game_id in game_ids}

        # Iterate through completed tasks as they finish
        for future in as_completed(futures):
            game_id = futures[future]
            try:
                # Retrieve the result from the completed task
                result = future.result()
                # Update the all_possessions_data with the possession data for the game_id
                all_possessions_data.update(result)
                processed_count += 1

                # Save the data to JSON every 5 processed games
                if processed_count % save_interval == 0:
                    with open(possessions_json, 'w') as file:
                        json.dump(all_possessions_data, file, indent=4)
                    print(f"Saved progress to JSON after processing {processed_count} games.")
            except Exception as e:
                print(f"Error processing game_id {game_id}: {e}")

    # Final save after all games are processed
    with open(possessions_json, 'w') as file:
        json.dump(all_possessions_data, file, indent=5)
    print("Final save completed after processing all games.")


if __name__ == "__main__":
    main()
    # defining window time
    # window_time = 10
    # # fetching game ids from WhoScored database
    # # game_ids = get_WhoScored_game_ids()
    # # Set up the WebDriver (for Chrome, but you can use any other browser)
    # options = Options()
    # options.add_argument("--start-maximized")  # Optional: Start the browser maximized
    # driver = webdriver.Chrome(options=options)
    #
    # finished_game_ids_json_path = r"C:\Users\user\Desktop\jsons\games_id.json"
    # new_games_ids_path = r"C:\Users\user\Desktop\jsons\new_games_id.json"
    # all_games_id_json_path = r"C:\Users\user\Desktop\jsons\all_games_id.json"
    #
    # # with open(all_games_id_json_path, 'r') as file:
    # #     all_games = json.load(file)
    # #
    # # with open(finished_game_ids_json_path, 'r') as file:
    # #     processed = json.load(file)
    # #
    # # with open(new_games_ids_path, 'r') as file:
    # #     unprocessed_games_id = json.load(file)
    # #
    # # for game_id in all_games:
    # #     if game_id not in processed:
    # #         unprocessed_games_id.append(game_id)
    # #
    # #
    # # # Write the updated data back to the JSON file
    # # with open(new_games_ids_path, 'w') as file:
    # #     json.dump(unprocessed_games_id, file, indent=1)
    #
    # # replace with your path
    # # file_path = r"C:\Users\user\Desktop\jsons\games_id.json"
    # with open(new_games_ids_path, 'r') as file:
    #     game_ids = json.load(file)
    # for game_id in game_ids:
    #     # built in this way: { minute (string) : [home team possession (double), away team possession (double)], ... }
    #     possessions_dict = get_possession(window_time, game_id, driver)
    #     # print(possessions_dict)
    #     # replace with your path
    #     ############# need to merge the two files!!!!!!####################
    #     possessions_json = r'C:\Users\user\Desktop\jsons\new_possessions_data.json'
    #     new_possessions_json = r'C:\Users\user\Desktop\jsons\new_possessions_data.json'
    #
    #     # Load the existing data
    #     with open(possessions_json, 'r') as file:
    #         data = json.load(file)
    #
    #     # If it's an empty dictionary, initialize it as an empty collection (for example, a dictionary)
    #     if not data:
    #         data = {}
    #
    #     data[game_id] = possessions_dict
    #
    #     # Write the updated data back to the JSON file
    #     with open(possessions_json, 'w') as file:
    #         json.dump(data, file, indent=5)
    #
    #     time_dict.clear()
    #
    # driver.quit()