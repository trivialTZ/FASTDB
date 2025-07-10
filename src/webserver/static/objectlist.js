import { fastdbap } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js"

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.ObjectList = class
{
    constructor( context, parentdiv )
    {
        this.context = context;
        this.topdiv = parentdiv;
    }

    render_page( data )
    {
        let self = this;

        // Calculate magnitudes assuming zp=31.4 (flux is nJy)
        data[ 'magmax' ] = []
        data[ 'magmaxerr' ] = []
        data[ 'maglast' ] = []
        data[ 'maglasterr' ] = []
        data[ 'magforcedlast' ] = []
        data[ 'magforcedlasterr' ] = []
        for ( let i in data.diaobjectid ) {
            if ( data.maxdetflux[i] > 0 ) {
                data.magmax.push( -2.5 * Math.log10( data.magdetflux[i] ) + 31.4 );
                data.magmaxerr.push( 2.5 / Math.log(10.) * data.magdetfluxerr[i] / data.magdetflux[i] );
            } else {
                data.magmax.push( -99 );
                data.magmaxerr.push( -99 );
            }
            if ( data.lastdetflux[i] > 0 ) {
                data.maglast.push( -2.5 * Math.log10( data.lastdetflux[i] ) + 31.4 );
                data.maglasterr.push( 2.5 / Math.log(10.) * data.lastdetfluxerr[i] / data.lastdetflux[i] );
            } else {
                data.magmax.push( -99 );
                data.magmaxerr.push( -99 );
            }
            if ( data.lastforcedflux[i] > 0 ) {
                data.magforcedlast.push( -2.5 * Math.log10( data.lastforcedflux[i] ) + 31.4 );
                data.magforcedlasterr.push( 2.5 / Math.log(10.) * data.lastforcedfluxerr[i] / data.lastforcedflux[i] );
            }
        }

        let fields = [ 'diaobjectid', 'ra', 'dec', 'ndet', 'mjd_n', 'band_n', 'mag_n', 'dmag_n',
                       'mjd_^', 'band_^', 'mag_^', 'dmag_^', 'mjd_fn', 'band_fn', 'mag_fn', 'dmag_fn' ]
        let fieldmap = { 'diaobjectid': 'diaobjectid',
                         'ra': 'ra',
                         'dec': 'dec',
                         'ndet': 'ndet',
                         'mjd_n': 'lastdetfluxmjd',
                         'band_n': 'lastdetfluxband',
                         'mag_n': 'maglast',
                         'dmag_n': 'maglasterr',
                         'mjd_^': 'maxdetfluxmjd',
                         'band_^': 'maxdetfluxband',
                         'mag_^': 'magmax',
                         'dmag_^': 'magmaxerr',
                         'mjd_fn': 'lastforcedfluxmjd',
                         'band_fn': 'lastforcedfluxband',
                         'mag_fn': 'magforcedlast',
                         'dmag_fn': 'magforcedlasterr' }
        
        let rowrenderer = function( data, i ) {
            let tr, td;
            tr = rkWebUtil.elemaker( tr, null );
            td = rkWebUtil.elemaker( td, tr, { "text": data.ra[i].toFixed(5) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.dec[i].toFixed(5) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.ndet[i].toString() } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.lastdetfluxmjd[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.lastdetfluxband[i] } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.maglast[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.maglasterr[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.maxdetfluxmjd[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.maxdetfluxband[i] } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.magmax[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.magmaxerr[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.lastforcedfluxmjd[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.lastforcedfluxband[i] } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.magforcedlast[i].toFixed(2) } );
            td = rkWebUtil.elemaker( td, tr, { "text": data.magforcedlasterr[i].toFixed(2) } );
            return tr;
        }

        this.objtable = rkWebUtil.SortableTable( data, rowrender, fields, { 'fieldmap': fieldmap,
                                                                            'dictoflists': true,
                                                                            'colorclasses': [ 'whitebg', 'greybg' ],
                                                                            'colrolength' : 3 } );
        // TODO: info about search criteria
        this.topdiv.appendChild( thisobjtable );
    }

    
}

// **********************************************************************
// Make it into a module

export { }
