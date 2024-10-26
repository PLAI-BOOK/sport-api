import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

# built in this way: { minute: [home team possession, away team possession], ... } IT'S ONLY NOW WITHOUT DATABASE!!!
time_dict = {}


def insert_window_possession(window_time, driver):
    """

    :param window_time: time window in minutes
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
    time_dict[window_time] = data_values


def move_window_left(number_of_steps, actions):
    """

    :param number_of_steps: number of minutes according to the time window.
    :param actions: gives access to the driver
    """
    for i in range(number_of_steps):
        actions.send_keys(Keys.ARROW_LEFT).perform()
        time.sleep(0.2)


def calculate_half_last_window(half_time, window_time, actions, driver):
    """

    :param half_time: number of minutes in the half
    :param window_time: the length of the window in minutes
    :param actions: element for preforming actions on the driver
    :param driver: webdriver
    :return: the length of the last window in this half because it's different than the regular time window.
    """
    insert_window_possession(half_time, driver)
    move_window_left(half_time % window_time, actions)
    return half_time % window_time


def access_to_time_slider(actions, driver):
    """
    take the keyboard to the right end of the time slider
    :param actions: element for preforming actions on the driver
    :param driver: webdriver
    """
    element = driver.find_element(By.XPATH, '/html/body/div[3]/div[5]/div[2]')
    time.sleep(0.5)
    actions.move_to_element(element).click().perform()
    time.sleep(0.5)

    # Send 'Tab' key twice
    actions.send_keys(Keys.TAB).perform()
    time.sleep(0.5)
    actions.send_keys(Keys.TAB).perform()
    time.sleep(0.5)
    actions.send_keys(Keys.TAB).perform()


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
        insert_window_possession(half_time + time_offset, driver)
        move_window_left(half_time, actions)
        return
    # if there is one window that should be shorter than the regular one we do it here
    if half_time % window_time != 0:
        last_window = calculate_half_last_window(half_time + time_offset, window_time, actions, driver)

    number_of_windows = half_time // window_time

    # in case of only one window when further calculations don't needed
    if number_of_windows == 1:
        insert_window_possession(half_time + time_offset, driver)
        move_window_left(window_time, actions)
        return
    # calculating the last movement to the previous half
    last_movement = half_time - (number_of_windows-1) * window_time - last_window + 1

    for i in range(number_of_windows):
        insert_window_possession(half_time + time_offset - last_window - i * window_time, driver)
        if i == number_of_windows - 1 and is_first_half and not is_over_time:
            return
        elif i == number_of_windows - 1:
            move_window_left(last_movement, actions)
        else:
            move_window_left(window_time, actions)
    # moving to the end of the previous half



def get_possession(window_time, first_half_minutes, second_half_minutes, game_id, overtime_first_half=0, overtime_second_half=0):
    """
    the main function that activates all the other functions.
    :param window_time: length of the time window in minutes
    :param first_half_minutes: length of the first half in minutes
    :param second_half_minutes: length of the first half in minutes
    :param game_id: string of the game id
    :param overtime_first_half: length of the overtime first half in minutes
    :param overtime_second_half: length of the overtime second half in minutes
    """
    if window_time >= 45:
        print("screw you, I'm not doing that window time")
        return

    # Set up the WebDriver (for Chrome, but you can use any other browser)
    options = Options()
    options.add_argument("--start-maximized")  # Optional: Start the browser maximized
    service = Service(executable_path="path_to_your_chromedriver")  # Update this path
    driver = webdriver.Chrome(options=options)

    # Open the target URL
    driver.get("https://www.whoscored.com/Matches/" + game_id + "/Live")

    # Wait for the page to load (increase time if necessary)
    time.sleep(3)

    try:
        actions = ActionChains(driver)
        access_to_time_slider(actions, driver)

        # overtime second half
        if overtime_first_half > 0:
            half_possessions(window_time, overtime_second_half, actions, driver, False, True)
            # overtime first half
            half_possessions(window_time, overtime_first_half, actions, driver, True, True)
        # second half
        half_possessions(window_time, second_half_minutes, actions, driver)
        # first half
        half_possessions(window_time, first_half_minutes, actions, driver, True)
        print(time_dict)

    except Exception as e:
        print(f"An error occurred: {e}")

    # Optionally, you can close the browser at the end
    driver.quit()
if __name__ == "__main__":
    get_possession(10, 46, 53, "1729540")
    #TODO: I don't see any DB and where to fetch it!!! when I'll have a table with half times and games ID I will fetch everything game by game using threads.
