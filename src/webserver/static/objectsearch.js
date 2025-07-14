import { fastdbap } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js";

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.ObjectSearch = class
{
    constructor( context, parentdiv )
    {
        this.context = context;
        this.topdiv = rkWebUtil.elemaker( "div", parentdiv, { "classes": [ "hbox", "xscroll" ] } );
    }


    render_page()
    {
        let self = this;
        let table, tr, td, div, p;

        rkWebUtil.wipeDiv( this.topdiv );

        // search by diaobject id

        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "searchinner", "xmarginright", "maxwcontent" ] } );
        p = rkWebUtil.elemaker( "p", div, { "text": "diaobjectid:" } );
        rkWebUtil.elemaker( "br", p );
        this.diaobjectid_widget = rkWebUtil.elemaker( "input", p, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.button( p, "Show", (e) => { self.show_object_info(); } );
        p = rkWebUtil.elemaker( "p", div );
        rkWebUtil.button( p, "Show Random Obj", (e) => { self.show_random_obj(); } );

        // search by ra/dec

        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "searchinner", "xmarginright", "maxwcontent" ] } );
        table = rkWebUtil.elemaker( "table", div, { "classes": [ "borderless" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "RA:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.ra_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": "°" } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Dec:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.dec_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": "°" } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "radius:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.radius_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": '"' } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.button( td, "Search", (e) => { self.object_search() } );
    }


    object_search()
    {
        let self = this;

        let procver = this.context.procver_widget.value;
        if ( procver == "—select one —" ) {
            alert( "Select a processing version to search" );
            return;
        }

        let searchcriteria = {};
        if ( this.ra_widget.value.trim().length > 0 )
            searchcriteria.ra = this.ra_widget.value.trim();
        if ( this.dec_widget.value.trim().length > 0 )
            searchcriteria.dec = this.dec_widget.value.trim();
        if ( this.radius_widget.value.trim().length > 0 )
            searchcriteria.radius = this.radius_widget.value.trim();

        rkWebUtil.wipeDiv( this.context.objectlistdiv );
        this.context.searchtabs.selectTab( "objectsearch" );
        rkWebUtil.elemaker( "p", this.context.objectlistdiv, { "text": "Searching for objects...",
                                                                "classes": [ "bold", "italic", "warning" ] } );
        this.context.connector.sendHttpRequest( "/objectsearch/" + procver, searchcriteria,
                                               (data) => { self.context.object_search_results(data); } );
    }


    show_object_info()
    {
        let self = this;
        let objid = this.diaobjectid_widget.value;
        let pv = this.context.procver_widget.value;

        rkWebUtil.wipeDiv( this.context.objectinfodiv );
        rkWebUtil.elemaker( "p", this.context.objectinfodiv,
                            { "text": "Loading object " + objid + " for processing version " + pv,
                              "classes": [ "warning", "bold", "italic" ] } );
        this.context.maintabs.selectTab( "objectinfo" );

        this.context.connector.sendHttpRequest( "/ltcv/getltcv/" + pv + "/" + objid, {},
                                                (data) => { self.actually_show_object_info( data ) } );
    }

    show_random_obj()
    {
        let self = this;
        let pv = this.context.procver_widget.value;

        rkWebUtil.wipeDiv( this.context.objectinfodiv );
        rkWebUtil.elemaker( "p", this.context.objectinfodiv,
                            { "text": "Loading random object for processing version " + pv,
                              "classes": [ "warning", "bold", "italic" ] } );
        this.context.maintabs.selectTab( "objectinfo" )

        this.context.connector.sendHttpRequest( "/ltcv/getrandomltcv/" + pv, {},
                                                (data) => { self.actually_show_object_info( data ) } );
    }

    actually_show_object_info( data )
    {
        let info = new fastdbap.ObjectInfo( data, this.context, this.context.objectinfodiv );
        info.render_page();
    }

}

// **********************************************************************
// Make it into a module

export { }
