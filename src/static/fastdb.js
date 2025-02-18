import { rkAuth } from "./rkauth.js";
import { rkWebUtil } from "./rkwebutil.js";

// Namespace

var fastdbap = {};


// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.Context = class
{
    constructor()
    {
        this.parentdiv = document.getElementById( "pagebody" );
        this.authdiv = document.getElementById( "authdiv" );
        this.maindiv = rkWebUtil.elemaker( "div", this.parentdiv, { 'id': 'parentdiv' } );
        this.frontpagediv = null;

        this.connector = new rkWebUtil.Connector( "/" );
    };

    init()
    {
        let self = this;
        this.auth = new rkAuth( this.authdiv, "",
                                () => { self.render_page(); },
                                () => { window.location.reload(); } );
        this.auth.checkAuth();
    };

    render_page()
    {
        let self = this;
        let p, span;

        if ( this.frontpagediv == null ) {
            rkWebUtil.wipeDiv( this.authdiv );
            p = rkWebUtil.elemaker( "p", this.authdiv,
                                    { "text": "Logged in as " + this.auth.username
                                      + "(" + this.auth.userdisplayname + ") — ",
                                      "classes": [ "italic" ] } );
            span = rkWebUtil.elemaker( "span", p,
                                       { "classes": [ "link" ],
                                         "text": "Log Out",
                                         "click": () => { self.auth.logout( ()=>{ window.location.reload(); } ) }
                                       } );
            this.frontpagediv = rkWebUtil.elemaker( "div", this.maindiv, { 'id': 'frontpagediv' } );
            p = rkWebUtil.elemaker( "p", this.frontpagediv, { "text": "Hello, world!" } );
        }
    };
}


// **********************************************************************
// **********************************************************************
// **********************************************************************

export { fastdbap };
