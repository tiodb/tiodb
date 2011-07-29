
def TioPluginMain(container_manager, parameters):

    print 'testplugin. parameters: %s' % parameters
		
    def Log(container, event_name, key, value, metadata):
        print 'testplugin.log: ', container, event_name, key, value, metadata
    
    container_manager.open('meta/containers').subscribe(Log)
      
        
