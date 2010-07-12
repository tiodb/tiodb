
  var log_element = null;
  var last_ret;

  function log(what, level) {
      if (log_element == null)
          return;

      str = level + ": " + what + "<br/>";
      log_element.innerHTML = str + log_element.innerHTML;
  }
  
  function log_error(what) {
      return log(what, "error");
  }

  function log_info(what) {
      return log(what, "info");
  }

  function log_result(what) {
      return log(what, "error");
  }

  function cancel_request(request) {
      request.abort();
  }

  function general_command(address, command, parameters, okCallback, errorCallback) {
      var xmlhttp = new XMLHttpRequest();

      full_command = address + "?command=" + command;
      if(parameters != "")
          full_command += "&" + parameters;

      log("general_command - " + full_command, "debug");

      xmlhttp.open("GET", full_command);

      xmlhttp.onreadystatechange = function() {
          if (xmlhttp.readyState == 4) {
              if (xmlhttp.status == 200) {
                  var ret = eval('(' + xmlhttp.responseText + ')');
                  ret.json = xmlhttp.responseText;
                  last_ret = ret.json;
              }
              else {
                  var ret = new Object();
                  ret.result = "error";
                  ret.description = xmlhttp.statusText;
              }

              if (ret.result == "ok")
                  okCallback(ret);
              else
                  errorCallback(ret);
          }
      }
      xmlhttp.send(null);
      
      return xmlhttp;
  }

  function container_command(address, command, container, parameters, okCallback, errorCallback) {
      x = "container=" + container;
      if (parameters != "")
          x += "&" + parameters;

      return general_command(address, command, x, okCallback, errorCallback);
  }
  
  function tio_ping(address, okCallback, errorCallback) {
      return general_command(address, "ping", "", okCallback, errorCallback);
  }

  function tio_full_query(address, container, okCallback, errorCallback, cookie) {
      parameters = '';
      if (cookie != null)
          parameters += "cookie=" + cookie;
      return container_command(address, "query", container, parameters, okCallback, errorCallback);
  }

  function tio_query(address, container, start, end, okCallback, errorCallback) {
      parameters = "start=" + start + "&end=" + end;
      return container_command(address, "query", container, parameters, okCallback, errorCallback);
  }

  function tio_get(address, container, key, okCallback, errorCallback, cookie) {
      parameters = "key=" + key
      
      if (cookie != null)
          parameters += "&cookie=" + cookie;

      return container_command(address, "get", container, parameters, okCallback, errorCallback);
  }

  function tio_get_count(address, container, okCallback, errorCallback) {
      return container_command(address, "get_count", container, "", okCallback, errorCallback);
  }
  
  function dump_result(result) {
    log_str = "result (" + result.result;
    
    if(result.result == "error")
        log_str += ": " + result.description;

    log_str += ")"; 

    log(log_str, "info");
}





