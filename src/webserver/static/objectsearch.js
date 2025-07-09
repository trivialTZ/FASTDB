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
        
        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "innerdiv", "xmarginright", "maxwcontent" ] } );
        p = rkWebUtil.elemaker( "p", div, { "text": "diaobjectid:" } );
        rkWebUtil.elemaker( "br", p );
        this.diaobjectid_widget = rkWebUtil.elemaker( "input", p, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.button( p, "Show", (e) => { alert("Not Implemented" ); } );

        // search by ra/dec
        
        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "innerdiv", "xmarginright", "maxwcontent" ] } );
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
        this.dec_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": '"' } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.button( td, "Search", (e) => { alert( "Not Implemented" ); } );
    }
}

// **********************************************************************
// Make it into a module

export { }
