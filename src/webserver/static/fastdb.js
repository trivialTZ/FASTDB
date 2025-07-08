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
        this.pagebody = document.getElementById( "pagebody" );
        this.authdiv = document.getElementById( "authdiv" );
        this.topbox = null;
        this.basicstats = null;
        this.objectsearch = null;
        this.maindiv = null;

        this.connector = new rkWebUtil.Connector( "/" );
    };

    init()
    {
        let self = this;
        this.auth = new rkAuth( this.authdiv, "",
                                () => { self.first_render_page(); },
                                () => { window.location.reload(); } );
        this.auth.checkAuth();
    };

    first_render_page()
    {
        let self = this;
        let p, span, wrapperdiv;

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

        rkWebUtil.wipeDiv( this.pagebody );

        // **********************************************************************
        // Top box has on the left a dropdown for selecting processing version,
        //    and some basic stats.  On the right, object search, maybe other things later.
        
        this.topbox = rkWebUtil.elemaker( "div", this.pagebody, { "classes": [ "topbox" ] } );

        // Basic stats
        
        this.basicstats = rkWebUtil.elemaker( "div", this.topbox, { "classes": [ "basicstats" ] } );
        rkWebUtil.elemaker( "h3", this.basicstats, { "text": "FASTDB" } );
        p = rkWebUtil.elemaker( "p", this.basicstats, { "text": "Processing version:" } );
        rkWebUtil.elemaker( "br", p );
        this.procver_widget = rkWebUtil.elemaker( "select", p, { "change": () => { self.change_procver(); } } );
        rkWebUtil.elemaker( "option", this.procver_widget, { "value": "—select one—",
                                                             "text": "—select one—",
                                                             "attributes": { "selected": 1 } } );
        this.connector.sendHttpRequest( "/getprocvers", {}, (data) => { self.populate_procver_widget(data); } );

        p = rkWebUtil.elemaker( "p", this.basicstats );
        this.objects_span = rkWebUtil.elemaker( "span", p, { "text": "— objects" } );
        rkWebUtil.elemaker( "br", p );
        this.sources_span = rkWebUtil.elemaker( "span", p, { "text": "— sources" } );
        rkWebUtil.elemaker( "br", p );
        this.forced_span = rkWebUtil.elemaker( "span", p, { "text": "— forced" } );
        
        // Object search
        
        this.objectsearch = rkWebUtil.elemaker( "div", this.topbox, { "classes": [ "objectsearch" ] } );
        rkWebUtil.elemaker( "h3", this.objectsearch, { "text": "Object Search" } );


        // **********************************************************************
        // Main div
        
        this.maindiv = rkWebUtil.elemaker( "div", this.pagebody, { "classes": [ "maindiv" ] } );
        p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Main div todo" } );
        for ( let i of Array(200).keys() ) {
            rkWebUtil.elemaker( "p", p, { "text": "Line " + i } );
        }
    };


    populate_procver_widget(data)
    {
        rkWebUtil.wipeDiv( this.procver_widget );
        rkWebUtil.elemaker( "option", this.procver_widget, { "value": "—select one—",
                                                             "text": "—select one—",
                                                             "attributes": { "selected": 1 } } );
        for ( let pv of data.procvers ) {
            rkWebUtil.elemaker( "option", this.procver_widget, { "value": pv, "text": pv } );
        }
    }


    change_procver()
    {
        let self = this;
        let pv = this.procver_widget.value;

        if ( pv == "—select one—" ) {
            this.objects_span.innerHTML = "— objects";
            this.sources_span.innerHTML = "— sources";
            this.forced_span.innerHTML = "— forced";
            return;
        }
        this.objects_span.innerHTML = "(loading...) objects";
        this.sources_span.innerHTML = "(loading...) sources";
        this.forced_span.innerHTML = "(loading...) forced";

        this.connector.sendHttpRequest( "/count/object/" + encodeURIComponent( pv ), {},
                                        (data) => {
                                            self.objects_span.innerHTML = data.count.toString() + " objects";
                                        } );
        this.connector.sendHttpRequest( "/count/source/" + encodeURIComponent( pv ), {},
                                        (data) => {
                                            self.sources_span.innerHTML = data.count.toString() + " sources";
                                        } );
        this.connector.sendHttpRequest( "/count/forced/" + encodeURIComponent( pv ), {},
                                        (data) => {
                                            self.forced_span.innerHTML = data.count.toString() + " forced";
                                        } );
    }
}


// **********************************************************************
// **********************************************************************
// **********************************************************************

export { fastdbap };
