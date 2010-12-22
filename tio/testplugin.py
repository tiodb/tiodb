
def TioPluginMain(container_manager):
    def Log(event_name, key, value, metadata):
        print event_name, key, value, metadata
    
    container_manager.open('meta/containers').subscribe(Log)
      
        