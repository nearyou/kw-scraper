function main(splash, args)
  -- Debug info - print arguments
  print("Starting Debug Lua script")
  print("Proxy:", args.proxy_url)

  -- Waits for the given condition to become true.
  function splash:wait_with_timeout(condition, timeout)
    timeout = timeout or 30
    local maxAttempts = timeout / 0.1
    local attempt = 0
    while attempt < maxAttempts do
      if condition() then return true end
      attempt = attempt + 1
      self:wait(0.1)
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
      if found then print("Text found:", text) end
      return found
    end
    return self:wait_with_timeout(condition)
  end

  function splash:is_forbidden()
    return splash.response_status_code == 403
  end
  
  -- Initialize the cookie jar
  splash:init_cookies({})

  -- Disable loading images
  splash.images_enabled = false
  
  -- 1. MIMIC CHROME HEADERS
  local headers = {
    ['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    ['Accept-Language'] = 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',

    ['Connection'] = 'keep-alive',
    ['Upgrade-Insecure-Requests'] = '1',
    ['Sec-Ch-Ua'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    ['Sec-Ch-Ua-Mobile'] = '?0',
    ['Sec-Ch-Ua-Platform'] = '"Windows"',
    ['Sec-Fetch-Dest'] = 'document',
    ['Sec-Fetch-Mode'] = 'navigate',
    ['Sec-Fetch-Site'] = 'none',
    ['Sec-Fetch-User'] = '?1'
  }
  
  splash:set_custom_headers(headers)
  splash:set_viewport_size(1920, 1080)
  
  -- Go to the search form page
  print("Going to search form page")
  
  -- Initial navigation
  local ok, reason = splash:go('https://ekw.ms.gov.pl/eukw_ogol/menu.do')
  if not ok then
    return {
      success = '0',
      code = 'initial-load-failed',
      reason = reason,
      html = splash:html()
    }
  end
  
  if splash:is_forbidden() then
    return {
      success = '0',
      code = 'forbidden-403',
      html = splash:html()
    }
  end

  -- Wait for menu page content
  if not splash:wait_for_text('Przeglądanie księgi wieczystej') then
    return {
      success = '0',
      code = 'menu-content-missing',
      html = splash:html()
    }
  end
  
  -- Update headers for next step
  headers['Referer'] = 'https://ekw.ms.gov.pl/eukw_ogol/menu.do'
  headers['Sec-Fetch-Site'] = 'same-origin'
  splash:set_custom_headers(headers)

  print("Going to wyszukiwanieKW page")
  local ok, reason = splash:go('https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW')
  if not ok then
    return {
      success = '0',
      code = 'search-load-failed',
      reason = reason,
      html = splash:html()
    }
  end
  
  if splash:is_forbidden() then
    return {
      success = '0',
      code = 'search-forbidden-403',
      html = splash:html()
    }
  end
  
  -- Wait for search page content
  if not splash:wait_for_text('Wróć do strony głównej') then
    return {
      success = '0',
      code = 'search-content-missing',
      html = splash:html()
    }
  end

  -- Check for critical elements
  local condition = function()
    local has_code_input = self:select('#kodWydzialuInput') ~= nil
    local has_number_input = self:select('input[name="numerKw"]') ~= nil
    local has_search_button = self:select('#wyszukaj') ~= nil
    return has_code_input and has_number_input and has_search_button
  end
    
  if not splash:wait_with_timeout(condition, 5) then
     return {
      success = '0',
      code = 'critical-elements-missing',
      html = splash:html()
    }
  end

  -- If we got here, everything is fine
  return {
    success = '1',
    code = 'ready-to-search'
  }
end
