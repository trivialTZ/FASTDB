import { fastdbap } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js"
import { SVGPlot } from "./svgplot.js"

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.ObjectInfo = class
{
    constructor( data, context, parentdiv )
    {
        this.data = data;
        this.context = context;
        this.parentdiv = parentdiv;

        this.data['s/n'] = [];
        for ( let i in this.data.psfflux ) {
            this.data['s/n'].push( this.data.psfflux[i] / this.data.psffluxerr[i] );
        }

        let knownbands = [ 'u', 'g', 'r', 'i', 'z', 'Y' ];
        let markerdict = { 'u': [ 'dot', 'circle' ],
                           'g': [ 'filledsquare', 'square' ],
                           'r': [ 'filleddiamond', 'diamond' ],
                           'i': [ 'filleduptriangle', 'uptriangle' ],
                           'z': [ 'filleddowntriangle', 'downtriangle' ],
                           'Y': [ 'filleddowntriangle', 'downtriangle' ]
                         };
        let colordict = { 'u': '#cc00cc',
                          'g': '#0000cc',
                          'r': '#00cc00',
                          'i': '#cc0000',
                          'z': '#888800',
                          'Y': '#884400'
                        };
        let othercolors = [ '#444400', '#440044', '#004444' ];
        let otherdex = 0;

        // Extract the data into svgplot datasets
        this.ltcvs = {}

        let unknownbands = [];
        for ( let i in data.mjd ) {
            if ( ! this.ltcvs.hasOwnProperty( data.band[i] ) ) {
                this.ltcvs[data.band[i]] = { 'undetected': { 'mjd': [],
                                                             'flux': [],
                                                             'dflux': []
                                                           },
                                             'detected': { 'mjd': [],
                                                           'flux': [],
                                                           'dflux': []
                                                         },
                                             'min': 1e32,
                                             'max': -1e32,
                                           };
                if ( knownbands.includes( data.band[i] ) ) {
                    this.ltcvs[data.band[i]].detmarker = markerdict[ data.band[i] ][0];
                    this.ltcvs[data.band[i]].nondetmarker = markerdict[ data.band[i] ][1];
                    this.ltcvs[data.band[i]].color = colordict[ data.band[i] ];
                } else {
                    unknownbands.push( data.band[i] );
                    this.ltcvs[data.band[i]].detmarker = 'dot';
                    this.ltcvs[data.band[i]].nondetmarker = 'circle';
                    this.ltcvs[data.band[i]].color = othercolors[ otherdex ];
                    otherdex += 1;
                    if ( otherdex >= othercolors.length ) otherdex = 0;
                }

            }
            let which = 'undetected';
            if ( data.isdet[i] ) which = "detected";
            this.ltcvs[data.band[i]][which].mjd.push( data.mjd[i] );
            this.ltcvs[data.band[i]][which].flux.push( data.psfflux[i] );
            this.ltcvs[data.band[i]][which].dflux.push( data.psffluxerr[i] );
            if ( data.psfflux[i] > this.ltcvs[data.band[i]].max )
                this.ltcvs[data.band[i]].max = data.psfflux[i];
            if ( data.psfflux[i] < this.ltcvs[data.band[i]].min )
                this.ltcvs[data.band[i]].min = data.psfflux[i];
        }

        this.allbands = [];
        for ( let b of knownbands ) if ( this.ltcvs.hasOwnProperty( b ) ) this.allbands.push( b );
        this.allbands = this.allbands.concat( unknownbands );
        this.shownbands = [...this.allbands];

        this.datasets = {};
        for ( let b of this.allbands ) {
            let curset = { 'rel': { 'detected': null, 'undetected': null },
                           'abs': { 'detected': null, 'undetected': null } };
            curset.abs.detected = new SVGPlot.Dataset( { 'caption': b,
                                                         'x': this.ltcvs[b].detected.mjd,
                                                         'y': this.ltcvs[b].detected.flux,
                                                         'dy': this.ltcvs[b].detected.dflux,
                                                         'marker': this.ltcvs[b].detmarker,
                                                         'linewid': 0,
                                                         'color': this.ltcvs[b].color
                                                       } )
            curset.abs.undetected = new SVGPlot.Dataset( { 'x': this.ltcvs[b].undetected.mjd,
                                                           'y': this.ltcvs[b].undetected.flux,
                                                           'dy': this.ltcvs[b].undetected.flux,
                                                           'marker': this.ltcvs[b].nondetmarker,
                                                           'linewid': 0,
                                                           'color': this.ltcvs[b].color
                                                         } );
            let detrely = [];
            let detreldy = [];
            for ( let i in this.ltcvs[b].detected.flux ) {
                detrely.push( this.ltcvs[b].detected.flux[i] / this.ltcvs[b].max );
                detreldy.push( this.ltcvs[b].detected.dflux[i] / this.ltcvs[b].max );
            }
            let nondetrely = [];
            let nondetreldy = [];
            for ( let i in this.ltcvs[b].undetected.flux ) {
                nondetrely.push( this.ltcvs[b].undetected.flux[i] / this.ltcvs[b].max );
                nondetreldy.push( this.ltcvs[b].undetected.dflux[i] / this.ltcvs[b].max );
            }
            curset.rel.detected = new SVGPlot.Dataset( { 'caption': b,
                                                         'x': this.ltcvs[b].detected.mjd,
                                                         'y': detrely,
                                                         'dy': detreldy,
                                                         'marker': this.ltcvs[b].detmarker,
                                                         'linewid': 0,
                                                         'color': this.ltcvs[b].color
                                                       } )
            curset.rel.undetected = new SVGPlot.Dataset( { 'x': this.ltcvs[b].undetected.mjd,
                                                           'y': nondetrely,
                                                           'dy': nondetreldy,
                                                           'marker': this.ltcvs[b].nondetmarker,
                                                           'linewid': 0,
                                                           'color': this.ltcvs[b].color
                                                         } );
            this.datasets[b] = curset;
        }
    }

    render_page()
    {
        let self = this;
        let topdiv, infodiv, ltcvdiv, table, tr, td, p;

        rkWebUtil.wipeDiv( this.parentdiv );
        topdiv = rkWebUtil.elemaker( "div", this.parentdiv, { "classes": [ "objectinfohbox" ] } );
        // TODO : maybe use a class other than "searchinner"?  It's got what I want,
        //   but is semantically weird
        infodiv = rkWebUtil.elemaker( "div", topdiv, { "classes": [ "searchinner", "yscroll", "xmarginright",
                                                                    "maxwcontent", "flexgrow0" ] } );
        ltcvdiv = rkWebUtil.elemaker( "div", topdiv, { "classes": [ "searchinner", "yscroll",
                                                                    "minwid0", "flexgrow1" ] } );

        // Object info on the left

        rkWebUtil.elemaker( "h4", infodiv, { "text": "diaobject " + this.data.objinfo.diaobjectid } );
        table = rkWebUtil.elemaker( "table", infodiv, { "classes": [ "borderless" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Processing Version:",
                                             "classes": [ "right", "xmarginright" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": this.data.objinfo.processing_version } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "RA:",
                                             "classes": [ "right", "xmarginright" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": this.data.objinfo.ra.toFixed(5) } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Dec:",
                                             "classes": [ "right", "xmarginright" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": this.data.objinfo.dec.toFixed(5) } );

        // Todo: info about nearby objects?  Probably need the server side to return that too

        let fields = [ 'mjd', 'band', 'psfflux', 'psffluxerr', 's/n', 'isdet' ];
        let hdrs = { 'psfflux': 'Flux (nJy)',
                     'psffluxerr': 'ΔFlux (nJy)',
                     'isdet': 'Detected?' }
        let rowrenderer = function( data, fields, i ) {
            let tr, td;
            tr = rkWebUtil.elemaker( "tr", null );
            let dettext = "";
            if ( data.isdet[i] ) dettext = "Yes";
            let args = {
                'mjd':        [ "td", tr, { "text": data.mjd[i].toFixed(2) } ],
                'band':       [ "td", tr, { "text": data.band[i] } ],
                'psfflux':    [ "td", tr, { "text": data.psfflux[i].toExponential(4) } ],
                'psffluxerr': [ "td", tr, { "text": data.psffluxerr[i].toExponential(4) } ],
                's/n':        [ "td", tr, { "text": data['s/n'][i].toFixed(1) } ],
                'isdet':      [ "td", tr, { "text": dettext } ],
            };
            for ( let f of fields ) {
                if ( !args.hasOwnProperty( f ) ) {
                    console.log( "ERROR: unknown field " + f + "; you should never see this!" )
                    continue;
                }
                td = rkWebUtil.elemaker.apply( null, args[f] );
            }
            return tr;
        };

        this.ltcvtable = new rkWebUtil.SortableTable( this.data, fields, rowrenderer,
                                                      { "dictoflists": true,
                                                        "hdrs": hdrs,
                                                        "colorclasses": [ "whitebg", "greybg" ],
                                                        "colorlength": 3 } );
        infodiv.appendChild( this.ltcvtable.table );


        // Lightcurve plots on right

        p = rkWebUtil.elemaker( "p", ltcvdiv, { "text": "Lightcurve display: " } );

        this.current_ltcv_display = "combined";
        this.ltcv_display_widget = rkWebUtil.elemaker( "select", p,
                                                       { "change": (e) => { self.update_ltcv_display() } } );
        rkWebUtil.elemaker( "option", this.ltcv_display_widget, { "value": "separate",
                                                                  "text": "separate" } );
        rkWebUtil.elemaker( "option", this.ltcv_display_widget, { "value": "combined",
                                                                  "text": "combined",
                                                                  "attributes": { "selected": 1 } } );

        rkWebUtil.elemaker( "text", p, { "text": "    y scale: " } );
        this.current_ltcv_yscale = "nJy";
        this.ltcv_yscale_widget = rkWebUtil.elemaker( "select", p,
                                                      { "change": (e) => { self.update_ltcv_display() } } );
        rkWebUtil.elemaker( "option", this.ltcv_yscale_widget, { "value": "nJy",
                                                                 "text": "nJy",
                                                                 "attributes": { "selected": 1 } } );
        rkWebUtil.elemaker( "option", this.ltcv_yscale_widget, { "value": "relative",
                                                                 "text": "relative" } )


        rkWebUtil.elemaker( "text", p, { "text": "   " } );
        this.show_nondet = true;
        this.show_nondet_checkbox = rkWebUtil.elemaker( "input", p,
                                                        { "id": "show-nondetections-checkbox",
                                                          "change": (e) => { self.update_ltcv_display() },
                                                          "attributes": { "type": "checkbox",
                                                                          "checked": 1 } } );
        rkWebUtil.elemaker( "label", p, { "text": "Show nondetections",
                                          "attributes": { "for": "show-nondetections-checkbox" } } );

        this.colorcheckboxp = rkWebUtil.elemaker( "p", ltcvdiv );

        this.ltcvs_div = rkWebUtil.elemaker( "div", ltcvdiv );

        // Make the plots

        this.render_ltcvs();
    }

    update_ltcv_display()
    {
        let mustchange = false;
        if ( this.ltcv_display_widget.value != this.current_ltcv_display ) {
            this.current_ltcv_display = this.ltcv_display_widget.value;
            mustchange = true;
        }
        if ( this.ltcv_yscale_widget.value != this.current_ltcv_yscale ) {
            this.current_ltcv_yscale = this.ltcv_yscale_widget.value;
            mustchange = true;
        }
        if ( this.show_nondet_checkbox.checked ) {
            if ( ! this.show_nondet ) {
                this.show_nondet = true;
                mustchange = true;
            }
        } else {
            if ( this.show_nondet ) {
                this.show_nondet = false;
                mustchange = true;
            }
        }
        let newshow = [];
        for ( let b of this.allbands ) {
            let show = this.band_checkboxes[b].checked;
            if ( ( (!show) && this.shownbands.includes(b) ) || ( show && (!this.shownbands.includes(b)) ) )
                mustchange = true;
            if ( show )
                newshow.push( b );
        }
        if ( mustchange ) {
            this.shownbands = newshow;
            this.render_ltcvs();
        }
    }


    render_ltcvs()
    {
        let self = this;

        rkWebUtil.wipeDiv( this.ltcvs_div );

        let minmaxmjd = this.get_min_max_mjd()
        let minmjd = minmaxmjd.min - 0.05 * ( minmaxmjd.max - minmaxmjd.min );
        let maxmjd = minmaxmjd.max + 0.05 * ( minmaxmjd.max - minmaxmjd.min );

        let ytitle = "flux (nJy)";
        if ( this.current_ltcv_yscale == 'relative' ) ytitle = "flux (rel.)";

        if ( this.current_ltcv_display == "combined" ) {
            let plot = new SVGPlot.Plot( { "name": "combined",
                                           "divid": "svgplotdiv_combined",
                                           "svgid": "svgplotsvg_combined",
                                           "title": null,
                                           "xtitle": "MJD",
                                           "ytitle": ytitle,
                                           "defaultlimits": [ minmjd, maxmjd, null, null ],
                                           "nosuppresszeroy": true,
                                           "zoommode": "default"
                                         } );
            for ( let b of this.shownbands ) {
                if ( this.current_ltcv_yscale == 'relative' ) {
                    plot.addDataset( this.datasets[b].rel.detected );
                    if ( this.snow_nondet ) plot.addDataset( this.datasets[b].rel.undetected );
                } else {
                    plot.addDataset( this.datasets[b].abs.detected );
                    if ( this.show_nondet ) plot.addDataset( this.datasets[b].abs.undetected );
                }
            }
            this.ltcvs_div.appendChild( plot.topdiv );
        }
        else {
            for ( let b of this.shownbands ) {
                let plot = new SVGPlot.Plot( { "name": b,
                                               "divid": "svgplotdiv_" + b,
                                               "svgid": "svgplotsvg_" + b,
                                               "title": b,
                                               "xtitle": "MJD",
                                               "ytitle": ytitle,
                                               "defaultlimits": [ this.minmjd, this.maxmjd, null, null ],
                                               "nosuppresszeroy": true,
                                               "zoommode": "default"
                                             } );
                if ( this.current_ltcv_yscale == 'relative' ) {
                    plot.addDataset( this.datasets[b].rel.detected );
                    if ( this.show_nondet ) plot.addDataset( this.datasets[b].rel.undetected );
                } else {
                    plot.addDataset( this.datasets[b].abs.detected );
                    if ( this.show_nondet ) plot.addDataset( this.datasets[b].abs.undetected );
                }
                this.ltcvs_div.appendChild( plot.topdiv );
            }
        }

        // Checkbox row at the top for selecting colors.  Do this after the plots
        //   so that all the stuff we need is defined.  (It may have already been...)

        rkWebUtil.wipeDiv( this.colorcheckboxp );
        rkWebUtil.elemaker( "text", this.colorcheckboxp, { "text": "Show Bands:  " } );
        this.band_checkboxes = {};
        for ( let b of this.allbands ) {
            let ds;
            if ( this.current_ltcv_yscale == "relative" )
                ds = this.datasets[b].rel.detected;
            else
                ds = this.datasets[b].abs.detected;
            let checkbox = rkWebUtil.elemaker( "input", this.colorcheckboxp,
                                               { "attributes": { "type": "checkbox" },
                                                 "id": "color-checkbox-" + b,
                                                 "change": (e) => { self.update_ltcv_display() } } );
            if ( this.shownbands.includes( b ) )
                checkbox.setAttribute( "checked", 1 );
            let label = rkWebUtil.elemaker( "label", this.colorcheckboxp,
                                            { "attributes": { "for": "color-checkbox-" + b } } );
            let span = rkWebUtil.elemaker( "span", label,
                                           { "attributes":
                                             { "style": "display: inline-block; color: " + ds.markercolor } } );
            let svg = SVGPlot.svg();
            svg.setAttribute( "width", "1ex" );
            svg.setAttribute( "height", "1ex" );
            svg.setAttribute( "viewBox", "0 0 10 10" );
            span.appendChild( svg );
            // let defs = rkWebUtil.elemaker( "defs", svg, { "svg": true } );
            // defs.appendChild( ds.marker );
            let polyline = rkWebUtil.elemaker( "polyline", svg,
                                               { "svg": true,
                                                 "attributes": {
                                                     "class": ds.name,
                                                     "points": "5,5",
                                                     "marker-start": "url(#" + ds.marker.id + ")",
                                                     "marker-mid": "url(#" + ds.marker.id + ")",
                                                     "marker-end": "url(#" + ds.marker.id + ")" } } );
            rkWebUtil.elemaker( "text", span, { "text": " " + b } );
            rkWebUtil.elemaker( "text", this.colorcheckboxp, { "text": "   " } );

            this.band_checkboxes[b] = checkbox;
        }

    }

    get_min_max_mjd()
    {
        if ( this.data.mjd.length == 0 ) return { 'min': 60000., 'max': 60001. };

        let minmjd = 1e32;
        let maxmjd = -1e32;
        for ( let i in this.data.mjd ) {
            if ( this.shownbands.includes( this.data.band[i] ) ) {
                if ( this.data.isdet[i] || this.show_nondet ) {
                    if ( this.data.mjd[i] < minmjd ) minmjd = this.data.mjd[i];
                    if ( this.data.mjd[i] > maxmjd ) maxmjd = this.data.mjd[i];
                }
            }
        }
        return { 'min': minmjd, 'max': maxmjd };
    }

}


// **********************************************************************
// Make it into a module

export { }
