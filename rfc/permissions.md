Adding Permissions resources
============================

Various implementations have requirements around forbidding the execution 
of some categories of data queries based on user roles.

The `RoleBasedAuthFilter` makes a global filter possible and the 
`RequestMapper` interface used in most of the Servlet classes allows
conditional query-based rejection of user requests.  

It would be better, however, to allow some query planning around what 
is and isn't available for reporting on.  To that end the following 
security oriented resources are proposed to support preflight 
authorization checks and corresponding messaging around getting 
authorization. 


New Resources
=============

[USERNAME] - refers to the authenticated name on the security context principal.  
This could be provided by an external auth layer based on the application
authentication mechanism. 

/users/[USERNAME]/access  - Describes the resources in the application 
which the user can query.

Sample document:
{
   tables: { "tablename1", "tablename2" }
}

This could be used as a white list or black list.  The presumption would
probably be that an unmentioned resource is fully available.

/users/[USERNAME]

Sample document:
{
    username: [USERNAME],
    access: {
        tables: { "tablename1", "tablename2" }
    }
}


Another possible extension:

/user -> alias to /user/[USERNAME FOR AUTHENTICTED_USER]

