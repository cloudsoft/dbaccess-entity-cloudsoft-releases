dbaccess
===

To build an assembly, run:

    mvn clean assembly:assembly

This creates a jar at:
    
    target/dbaccess-0.1.0-SNAPSHOT.jar

which can be placed in:

    $BROOKLYN_HOME/lib/dropins
 

### Opening in an IDE

To open this project in an IDE, you will need maven support enabled
(e.g. with the relevant plugin).  You should then be able to develop
it and run it as usual.  For more information on IDE support, visit:

    https://brooklyn.incubator.apache.org/v/latest/dev/env/ide/


### Customizing the Assembly

The artifacts (directory and tar.gz by default) which get built into
`target/` can be changed.  Simply edit the relevant files under
`src/main/assembly`.


### More About Apache Brooklyn

Apache Brooklyn is a code library and framework for managing applications in a 
cloud-first dev-ops-y way.  It has been used to create this sample project 
which shows how to define an application and entities for Brooklyn.

This project can be extended for more complex topologies and more 
interesting applications, and to develop the policies to scale or tune the 
deployment depending on what the application needs.

For more information consider:

* Visiting the Apache Brooklyn home page at https://brooklyn.incubator.apache.org
* Finding us on IRC #brooklyncentral or email (click "Community" at the site above) 
* Forking the project at  http://github.com/apache/incubator-brooklyn/

A sample Brooklyn project should specify its license.

