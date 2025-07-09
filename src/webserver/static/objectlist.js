import { fastdb } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js"

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdb.ObjectList = class
{
    constructor( context, parentdiv )
    {
        this.context = context;
        this.topdiv = parentdiv;
    }

    render_page( data )
    {
        let self = this;
    }

    
}

// **********************************************************************
// Make it into a module

export { }
