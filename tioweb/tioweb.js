
  var log_element;

  function log(what, level) {
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

  function tio_full_query(address, container, okCallback, errorCallback) {
      return container_command(address, "query", container, "", okCallback, errorCallback);
  }

  function tio_query(address, container, start, end, okCallback, errorCallback) {
      parameters = "start=" + start + "&end=" + end;
      return container_command(address, "query", container, parameters, okCallback, errorCallback);
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
