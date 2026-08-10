function main(splash, args)
  -- Debug info - print arguments
  print("Starting Lua script with code:", args.code, "number:", args.number, "control:", args.control)
  
  -- Check if we should only fetch cookies
  local fetch_cookies_only = args.fetch_cookies_only or true
  
  -- Check if this is a health check
  local health_check_only = args.health_check_only or false

  -- Waits for the given condition to become true.
  -- Times out after given time (30s by default, increased for stability)
  function splash:wait_with_timeout(condition, timeout)
    timeout = timeout or 30  -- Increased from 15s to 30s
    local maxAttempts = timeout / 0.02
    local attempt = 0
    while attempt < maxAttempts do
      if condition() then return true end
      attempt = attempt + 1
      self:wait(0.02)
    end
    print("Timeout waiting for condition after " .. timeout .. " seconds")
    return false
  end

  -- Waits until given text appears on the page
  function splash:wait_for_text(text)
    print("Waiting for text:", text)
    local condition = function ()
      local html = self:html()
      local found = html:find(text)
      local rejected = self:is_rejected() 
      local not_found = self:is_not_found()
      
      if found then print("Text found:", text) end
      if rejected then print("Request rejected") end
      if not_found then print("Book not found") end
      
      return found or rejected or not_found
    end
    return self:wait_with_timeout(condition)
  end

  -- Waits until element selected by given selector
  -- appears on the page
  function splash:wait_for_element(selector)
    print("Waiting for element:", selector)
    local condition = function ()
      local element = self:select(selector)
      if element then print("Element found:", selector) end
      return element ~= nil
    end
    return self:wait_with_timeout(condition)
  end

  -- Waits for a text fields to appear and then send keys to it
  function splash:wait_send_keys(selector, keys)
    print("Waiting to send keys to:", selector, "Keys:", keys)
    if self:wait_for_element(selector) then
      return self:select(selector):send_keys(keys)
    end
    print("Failed to send keys to:", selector)
    return false
  end

  -- Waits for an element to appear and then clicks on it
  function splash:wait_mouse_click(selector)
    print("Waiting to click on:", selector)
    if self:wait_for_element(selector) then
      print("Clicking on:", selector)
      return self:select(selector):mouse_click()
    end
    print("Failed to click on:", selector)
    return false
  end

  -- Waits for the TSPD_101 cookie to appear in the cookie jar
  function splash:wait_for_tspd_cookie()
    print("Waiting for TSPD_101 cookie")
    local condition = function ()
      for index, cookie in pairs(self:get_cookies()) do
        if cookie['name'] == 'TSPD_101' then 
          print("TSPD_101 cookie found")
          return true 
        end
      end
      return false
    end
    return self:wait_with_timeout(condition)
  end

  function splash:is_forbidden()
    return splash.response_status_code == 403
  end

  function splash:is_rejected()
    local html = self:html()
    local rejected = html:find('The requested URL was rejected. Please consult with your administrator.')
    if rejected then print("Request was rejected") end
    return rejected
  end

  function splash:is_not_found()
    local html = self:html()
    local not_found = html:find('nie została odnaleziona')
    if not_found then print("Book was not found") end
    return not_found
  end

  function splash:is_maintenance_mode()
    local html = self:html()
    -- Check for explicit maintenance indicators only
    local maintenance_indicators = {
      'przerwa serwisowa',
      'serwis techniczny',
      'maintenance',
      'czasowo niedostępna',
      'temporarily unavailable'
    }
    
    for _, indicator in ipairs(maintenance_indicators) do
      if html:find(indicator) then
        print("Maintenance detected: found", indicator)
        return true
      end
    end
    
    -- Do NOT check for missing form elements here
    -- Missing elements could be due to slow loading or network issues
    -- Only detect maintenance when explicit maintenance messages are present
    
    return false
  end
  
  function splash:check_for_critical_elements_with_timeout()
    -- Wait up to 30 seconds for critical form elements to appear
    -- Only declare maintenance if they don't appear after 30 seconds
    print("Checking for critical elements with 30s timeout...")
    local timeout = 30
    local condition = function()
      local has_code_input = self:select('#kodWydzialuInput') ~= nil
      local has_number_input = self:select('input[name="numerKw"]') ~= nil
      local has_search_button = self:select('#wyszukaj') ~= nil
      
      if has_code_input and has_number_input and has_search_button then
        print("Critical elements found")
        return true
      end
      return false
    end
    
    local found = self:wait_with_timeout(condition, timeout)
    if not found then
      print("Critical elements not found after 30 seconds - possible maintenance")
    end
    return found
  end

  local function error_response(e)
    print("Returning error response:", e)
    return {
      success = '0',
      code = e,
      cookies = splash:get_cookies(),  -- Direct cookie access
    }
  end

  local function bail_response(e)
    print("Returning bail response:", e)
    return {
      success = '2',
      code = e,
      cookies = splash:get_cookies(),  -- Direct cookie access
    }
  end

  local function get_section(buttonSelector, headerText)
    print("Getting section with button:", buttonSelector, "header:", headerText)
    if not splash:wait_mouse_click(buttonSelector) then
      print("Failed to click button:", buttonSelector)
      error({code = 'click-failed-on-button'})
    end
    
    if not splash:wait_for_text(headerText) then
      print("Header text not found:", headerText)
      error({code = 'header-not-found'})
    end
    
    if not splash:wait_for_element('input.text1') then
      print("Text1 input not found")
      error({code = 'input-not-found'})
    end
    
    if splash:is_rejected() then 
      print("Request was rejected")
      error({code = 'request-rejected'}) 
    end
    
    print("Section loaded successfully")
    return splash:html()
  end

  -- Initialize the cookie jar
  print("Initializing cookies")
  if args.cookies ~= nil then
    splash:init_cookies(args.cookies)
    print("Using provided cookies")
  else
    splash:init_cookies({})
    print("No cookies provided, using empty cookie jar")
  end

  -- Disable loading images
  splash.images_enabled = false
  print("Image loading disabled")
  
  -- 1. MIMIC CHROME HEADERS
  -- These headers tell the server: "I am Chrome running on Windows 10"
  local headers = {
    ['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    ['Accept-Language'] = 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7', -- CRITICAL for Polish gov sites
    ['Accept-Encoding'] = 'gzip, deflate, br',
    ['Connection'] = 'keep-alive',
    ['Upgrade-Insecure-Requests'] = '1',
    ['Sec-Ch-Ua'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    ['Sec-Ch-Ua-Mobile'] = '?0',
    ['Sec-Ch-Ua-Platform'] = '"Windows"',
    ['Sec-Fetch-Dest'] = 'document',
    ['Sec-Fetch-Mode'] = 'navigate',
    ['Sec-Fetch-Site'] = 'none', -- 'none' because you are typing the URL directly first
    ['Sec-Fetch-User'] = '?1'
  }
  
  -- Apply the headers
  splash:set_custom_headers(headers)
  print("Custom Chrome 120 headers set")
  
  -- 2. SET VIEWPORT
  -- Real users don't have 0x0 screens. Set a standard 1080p resolution.
  splash:set_viewport_size(1920, 1080)
  print("Viewport set to 1920x1080")
  
  -- Go to the search form page
  print("Going to search form page")
  local ok, reason = splash:go('https://ekw.ms.gov.pl/eukw_ogol/menu.do')
  if not ok then
    local error_msg = reason or "unknown network error"
    print("Failed to load initial page: " .. tostring(error_msg))
    return error_response('page-load-failed')
  end
  
  -- Check for 403 Forbidden status
  if splash:is_forbidden() then
    print("ERROR: Received HTTP 403 Forbidden from the server")
    return {
      success = '0',
      code = 'forbidden-403',
      message = "Server returned 403 Forbidden - your IP or proxy may be blocked",
      cookies = splash:get_cookies(),  -- Direct cookie access
    }
  end
  

  -- Wait for menu page content (replaces assert with graceful handling)
  if not splash:wait_for_text('Przeglądanie księgi wieczystej') then
    print("Menu page text not found: Przeglądanie księgi wieczystej")
    return {
      success = '0',
      code = 'menu-page-content-missing',
      html = splash:html(),  -- Return the HTML for debugging
      cookies = splash:get_cookies()
    }
  end

  if splash:is_rejected() then 
    print("Request rejected at menu page")
    return error_response('request-rejected0') 
  end
  
  -- Update headers to show we came from the same site (click, not direct entry)
  -- We update the existing headers table to preserve User-Agent etc.
  headers['Referer'] = 'https://ekw.ms.gov.pl/eukw_ogol/menu.do'
  headers['Sec-Fetch-Site'] = 'same-origin'
  splash:set_custom_headers(headers)
  print("Updated headers with Referer and Sec-Fetch-Site: same-origin")

  print("Going to wyszukiwanieKW page")
  local ok, reason = splash:go('https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW')
  if not ok then
    local error_msg = reason or "unknown network error"
    print("Failed to load search page: " .. tostring(error_msg))
    return error_response('search-page-load-failed')
  end
  
  -- Check for 403 Forbidden on search page
  if splash:is_forbidden() then
    print("ERROR: Received HTTP 403 Forbidden on search page")
    return {
      success = '0',
      code = 'search-page-forbidden-403',
      message = "Search page returned 403 Forbidden - your IP or proxy may be blocked",
      cookies = splash:get_cookies(),  -- Direct cookie access
    }
  end

  
  -- Wait for search page content (replaces assert with graceful handling)
  if not splash:wait_for_text('Wróć do strony głównej') then
    print("Search page text not found: Wróć do strony głównej")
    return {
      success = '0',
      code = 'search-page-content-missing',
      html = splash:html(),  -- Return the HTML for debugging as requested
      cookies = splash:get_cookies()
    }
  end

  if splash:is_rejected() then 
    print("Request rejected at search page")
    return error_response('request-rejected1') 
  end
  
  -- Check for maintenance mode after loading the search page
  if splash:is_maintenance_mode() then
    print("Maintenance mode detected")
    return error_response('maintenance-mode')
  end
  
  -- Check if critical elements are present (with 30s timeout)
  if not splash:check_for_critical_elements_with_timeout() then
    print("Critical elements missing after 30s - maintenance mode")
    return error_response('maintenance-mode')
  end
  
  -- If this is a health check, we've verified the site is working
  if health_check_only then
    print("Health check completed - site is working")
    return {
      success = '1',
      code = 'site-healthy',
      cookies = splash:get_cookies()
    }
  end
  
  assert(splash:wait_for_tspd_cookie())
  print("TSPD cookie acquired")

  -- Fill the search form
  print("Filling search form")
  assert(splash:wait_send_keys('#kodWydzialuInput', args.code))
  assert(splash:wait_send_keys('input[name="numerKw"]', args.number))
  assert(splash:wait_send_keys('input[name="cyfraKontrolna"]', args.control))
  print("Form filled: code:", args.code, "number:", args.number, "control:", args.control)

  -- Submit the form
  print("Submitting search form")
  if not splash:wait_mouse_click('#wyszukaj') then
    print("Failed to click search button")
    return error_response('search-button-click-failed')
  end

  -- Wait for results with extended timeout
  print("Waiting for search results")
  if not splash:wait_for_text('Wynik wyszukiwania księgi wieczystej') then
    print("Search results page did not load")
    return error_response('search-results-timeout')
  end
  
  if not splash:wait_for_text('PROJEKT WSPÓŁFINANSOWANY ZE ŚRODKÓW UNII EUROPEJSKIEJ W RAMACH EUROPEJSKIEGO FUNDUSZU SPOŁECZNEGO') then
    print("EU funding text not found - possible incomplete page load")
    -- Don't fail here, continue as the main content might still be valid
  end
  
  -- Check for error conditions
  if splash:is_rejected() then 
    print("Request rejected after search")
    return error_response('request-rejected') 
  end
  
  -- Check for maintenance mode after search
  if splash:is_maintenance_mode() then
    print("Maintenance mode detected after search")
    return error_response('maintenance-mode')
  end
  
  -- Better check for "not found" case
  if splash:html():find('nie została odnaleziona') then 
    print("Book not found! Setting not-found code")
    return {
      success = '0',
      code = 'not-found',
      cookies = splash:get_cookies(),  -- Direct cookie access
    }
  end

  main = splash:html()
  print("Main page content captured")
  
  -- If we're only fetching cookies, return them now
  if fetch_cookies_only then
    print("Fetch cookies only mode - returning cookies after successful search")
    return {
      success = '1',
      code = 'cookies-acquired',
      cookies = splash:get_cookies()
    }
  end

  local response = {
    success = '1', 
    main = main, 
    zeroth = '', 
    first = '', 
    second = '', 
    third = '', 
    fourth = '',
    cookies = splash:get_cookies()  -- Direct cookie access
  }

  -- Check if standard view button is available
  print("Checking for standard view button")
  if splash:select('button[name="przyciskWydrukZwykly"]') == nil then
    print("Standard view button not found")
    return bail_response("bail")
  end
  print("Standard view button found")

  -- Get all book sections for standard view
  print("Getting all book sections")
  local sections = {
    {'zeroth', 'button[name="przyciskWydrukZwykly"]', 'OZNACZENIE'},
    {'first', 'input[value="Dział I-Sp"]', 'SPIS PRAW'},
    {'second', 'input[value="Dział II"]', 'WŁASNOŚĆ'},
    {'third', 'input[value="Dział III"]', 'PRAWA, ROSZCZENIA I OGRANICZENIA'},
    {'fourth', 'input[value="Dział IV"]', 'HIPOTEKA'},
  }

  for index, section in ipairs(sections) do
    print("Processing section:", section[1], "with selector:", section[2])
    local success, result = pcall(get_section, section[2], section[3])
    if not success then
      print("Failed to get section:", section[1], "Error:", result.code)
      return error_response(result.code)
    end
    print("Section", section[1], "loaded successfully")
    response[section[1]] = result
  end

  print("All sections loaded successfully")
  
  -- Prevent potential recursive structures and limit string sizes
  for section_key, section_content in pairs(response) do
    -- Skip cookies field which we already processed
    if section_key ~= 'cookies' and type(section_content) == 'string' then
      -- Limit string length if needed (e.g., to 1MB)
      if #section_content > 1048576 then  -- 1MB limit
        print("Warning: truncating section " .. section_key .. " from " .. #section_content .. " to 1MB")
        response[section_key] = string.sub(section_content, 1, 1048576) .. "... [content truncated]"
      end
    end
  end
  
  -- Make sure cookies are always directly included
  response.cookies = splash:get_cookies()
  
  -- Count cookies for logging
  local cookie_count = 0
  if type(response.cookies) == "table" then
    for _ in pairs(response.cookies) do
      cookie_count = cookie_count + 1
    end
  end
  
  print("Returning response with " .. cookie_count .. " cookies")
  
  -- Return the response with simplified structure
  return response
end
