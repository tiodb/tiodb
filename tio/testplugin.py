
def Log(container, event_name, key, value, metadata):
    print 'testplugin.log: ', container.name, event_name, key, value, metadata

def TioPluginMain(container_manager, parameters):

    global g_container_manager
    
    g_container_manager = container_manager

    print 'testplugin. parameters: %s' % parameters
    
    def OnNewContainer(container, event_name, key, value, metadata):
        if key and key[:5] != 'meta/':
            print 'subscribing to ', key
            g_container_manager.open(key).subscribe(Log)
    
    container_manager.open('meta/containers').subscribe(OnNewContainer)
      
        
