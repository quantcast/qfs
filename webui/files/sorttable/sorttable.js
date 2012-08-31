/*
Table sorting script, taken from http://www.kryogenix.org/code/browser/sorttable/ .
Distributed under the MIT license: http://www.kryogenix.org/code/browser/licence.html .

Adaptation by Joost de Valk ( http://www.joostdevalk.nl/ ) to add alternating row classes as well.

Copyright (c) 1997-2006 Stuart Langridge, Joost de Valk.
*/

/* Change these values */

var sort_link = 0;
var interval = 60;
var timerId = -1;

/* Don't change anything below this unless you know what you're doing */
//addEvent(window, "load", sortables_init);
addEvent(window, "load", on_window_load);

var SORT_COLUMN_INDEX;

function onFormChange(){
    if(timerId != -1){
         clearTimeout(timerId);
         timerId = -1;
    } 

    var refreshInterval = 0;        
    var intervalChanged = 0;
    
    var refreshElem=document.getElementById("refresh");
    if(refreshElem != null && refreshElem != "") {
           refreshInterval = refreshElem.value;
            if (refreshInterval != interval) {
                interval = refreshInterval;
                intervalChanged = 1;
            }
    }
      
    if( hasDataInStartTime() != 0) {
        return;
    }
        
    if (interval > 0){
        newInterval = interval;
        if(intervalChanged == 0 && interval <120)
            newInterval = 120;
        timerId = setTimeout('sendPost()', newInterval*1000);
    }
}

function sendPost() {
    // autorefresh
    var myForm = document.getElementById('selectHeaderForm');
    timerId = -1; 
    if(myForm != null) {
        if(interval > 0)   // start new timer           
           timerId = setTimeout('sendPost()', interval*1000);
        myForm.submit();
    }
}


function postCharts() {
    // autorefresh
    if(timerId != -1){
         clearTimeout(timerId);
         timerId = -1;
    } 
    var myForm = document.getElementById('selectHeaderForm');
    if(myForm != null) {
        var hiddenInput = document.getElementById('chartInput');
        if(hiddenInput != null) {
            hiddenInput.value = "GETCHART";
        }
        myForm.submit();
    }
}

function hasDataInStartTime(){
    var bRet = 0
    var minusTimeElem=document.getElementById("startTime");
    if(minusTimeElem != null && minusTimeElem != "") {
        var itm = minusTimeElem.value;
        if(itm != "" && itm != "0") 
            bRet = 1;
    }
    return(bRet);
}    

function on_window_load() {

    if(timerId != -1){
        clearTimeout(timerId);
        timerId = -1;
    }

    sortables_init();
    
    var myForm = document.getElementById('selectHeaderForm');
    if(myForm == null) {
        return;
    }
    
    myForm.onchange = onFormChange;
        
    var chartButton = document.getElementById('chartButton');
    if(chartButton != null) {
        chartButton.onclick = postCharts;
    }
    
    var refreshElem=document.getElementById("refresh");
    if(refreshElem != null && refreshElem != "") {
           interval = refreshElem.value;
           if (interval < 0)
                interval = 0;
    }

    if(hasDataInStartTime() == 0 && interval > 0) {
        timerId = setTimeout('sendPost()', interval*1000);
    }

//     alert("doooone");
}




/*----- sorting ---------------------------------------------------------------------*/

function sortables_init() {
	// Find all tables with class sortable and make them sortable
	if (!document.getElementsByTagName) return;
	tbls = document.getElementsByTagName("table");
	for (ti=0;ti<tbls.length;ti++) {
		thisTbl = tbls[ti];
		if (((' '+thisTbl.className+' ').indexOf("sortable") != -1) && (thisTbl.id)) {
			//initTable(thisTbl.id);
			ts_makeSortable(thisTbl);
			//sum_up(thisTbl);
		}
	}
}

function ts_makeSortable(table) {
	if (table.rows && table.rows.length > 0) {
		var firstRow = table.rows[0];
	}
	if (!firstRow) return;

	var sTableCell = table.id + "sortCell"
	var sortedName = getCookie(sTableCell);
        var sortedCell = null;
        
        
	// We have a first row: assume it's the header, and make its contents clickable links
	for (var i=0;i<firstRow.cells.length;i++) {
		var cell = firstRow.cells[i];
		var txt = ts_trim(ts_getInnerText(cell));
                if(sortedName != null && txt == sortedName){
                    sortedCell = cell;
                }
		if (cell.className != "unsortable" && cell.className.indexOf("unsortable") == -1) {
			cell.innerHTML = '<a href="#" class="theBlueClr" onclick="ts_resortTable(this);return false;">'+txt+'</a>';
		}
	}
        
        if(sortedCell != null){
            var sTableSortdown = table.id + "sortDown"
            var sortDown = getCookie(sTableSortdown);
            if(sortDown != null && sortDown == "0"){
                sortedCell.setAttribute('sortdir','down'); // need to set it to 'down' to make 'up' sorting
            }
            ts_resortTableByCell(sortedCell);
        } else {
       	    alternate(table);
        }
        
}

function ts_getInnerText(el) {
	if (typeof el == "string") return el;
	if (typeof el == "undefined") { return el };
	if (el.innerText) return el.innerText;	//Not needed but it is faster
	var str = "";
	
	var cs = el.childNodes;
	var l = cs.length;
	for (var i = 0; i < l; i++) {
		switch (cs[i].nodeType) {
			case 1: //ELEMENT_NODE
				str += ts_getInnerText(cs[i]);
				break;
			case 3:	//TEXT_NODE
				str += cs[i].nodeValue;
				break;
		}
	}
	return str;
}

function ts_trim(str) {
    var newstr = "";
    if(str != "")
        newstr = str.replace(/^\s+|\s+$/g, ''); 
    return newstr;
}


function ts_resortTableByCell(cell) {
    var len = cell.childNodes.length;
    var lnk = null;
    for (i=0; i< len; i++){
        if(cell.childNodes[i].nodeName=="A"){
            lnk = cell.childNodes[i];
            break;
        }
    }
    
    if(lnk != null){
        sort_link = lnk;
        ts_resortTable(lnk);
    }
}                

function ts_resortTable(lnk) {

        var cell=lnk.parentNode;
	var column = cell.cellIndex;
	var table = getParent(cell,'TABLE');
        var down = 1;
	
	// Work out a type for the column
	var theLen = table.rows.length;
	if (theLen <= 1) return;

//            alert(table.rows[1].className);
        
        var iIndex = -1;
	for (var i = 1; i < theLen; i++){
            if(table.rows[i].className != 'totalCls') { 
                iIndex = i;
                break;
             }
        }        
        if(iIndex == -1 || iIndex >= theLen) return;
        
       	var itm = ts_trim(ts_getInnerText(table.rows[iIndex].cells[column]));
	sortfn = ts_sort_caseinsensitive;
	if (!isNaN(itm)) {
            sortfn = ts_sort_numeric;
        } else {        
            sortfn = ts_sort_caseinsensitive;
        }
//	if (itm.match(/^\d\d[\/-]\d\d[\/-]\d\d\d\d$/)) sortfn = ts_sort_date;
//	if (itm.match(/^\d\d[\/-]\d\d[\/-]\d\d$/)) sortfn = ts_sort_date;
//	if (itm.match(/^[£$€]/)) sortfn = ts_sort_currency;
//	if (itm.match(/^[+-]?\d*\.?\d+$/)) sortfn = ts_sort_numeric;
//	if (itm.match(/^[+-]?\d*\.\d+e[+-]\d+$/)) sortfn = ts_sort_numeric;

	SORT_COLUMN_INDEX = column;
	var newRows = new Array();
	for (j=iIndex;j<theLen;j++) { 
		newRows[j-iIndex] = table.rows[j];
	}

	newRows.sort(sortfn);

     // if resorting the same cell    
        if(sort_link != 0 && sort_link == lnk &&
           cell.getAttribute("sortdir") == 'down') 
           down = 0;
           
        if(down == 0) {
                        // switching to 'up' sort        
			cell.setAttribute('sortdir','up');
	} else {
                        // switching to 'down' sort        
			cell.setAttribute('sortdir','down');
			newRows.reverse();
	} 
	
    // We appendChild rows that already exist to the tbody, so it moves them rather than creating new ones
    // don't do sortbottom rows
        for (i=0; i<newRows.length; i++) { 
		if (!newRows[i].className || (newRows[i].className && (newRows[i].className.indexOf('sortbottom') == -1))) {
			table.tBodies[0].appendChild(newRows[i]);
		}
	}
    // do sortbottom rows only
       for (i=0; i<newRows.length; i++) {
		if (newRows[i].className && (newRows[i].className.indexOf('sortbottom') != -1)) 
			table.tBodies[0].appendChild(newRows[i]);
	}
    

        if(sort_link != 0 && sort_link != lnk)
            sort_link.className = "theBlueClr";

        lnk.className = "theRedClr";
        
        sort_link = lnk;

        var txt = ts_trim(ts_getInnerText(cell));
        var sTableCell = table.id + "sortCell"
        var sTableSortdown =  table.id + "sortDown"
        createCookie(sTableCell, txt, 365);
        createCookie(sTableSortdown, down, 365);

	alternate(table);		
}

function getParent(el, pTagName) {
	if (el == null) {
		return null;
	} else if (el.nodeType == 1 && el.tagName.toLowerCase() == pTagName.toLowerCase()) {	// Gecko bug, supposed to be uppercase
		return el;
	} else {
		return getParent(el.parentNode, pTagName);
	}
}

function ts_sort_date(a,b) {
	// y2k notes: two digit years less than 50 are treated as 20XX, greater than 50 are treated as 19XX
	aa = ts_getInnerText(a.cells[SORT_COLUMN_INDEX]);
	bb = ts_getInnerText(b.cells[SORT_COLUMN_INDEX]);
	if (aa.length == 10) {
			dt1 = aa.substr(6,4)+aa.substr(3,2)+aa.substr(0,2);
	} else {
			yr = aa.substr(6,2);
			if (parseInt(yr) < 50) { 
				yr = '20'+yr; 
			} else { 
				yr = '19'+yr; 
			}
			dt1 = yr+aa.substr(3,2)+aa.substr(0,2);
	}
	if (bb.length == 10) {
			dt2 = bb.substr(6,4)+bb.substr(3,2)+bb.substr(0,2);
	} else {
			yr = bb.substr(6,2);
			if (parseInt(yr) < 50) { 
				yr = '20'+yr; 
			} else { 
				yr = '19'+yr; 
			}
			dt2 = yr+bb.substr(3,2)+bb.substr(0,2);
	}
	if (dt1==dt2) {
		return 0;
	}
	if (dt1<dt2) { 
		return -1;
	}
	return 1;
}

function ts_sort_currency(a,b) { 
	aa = ts_getInnerText(a.cells[SORT_COLUMN_INDEX]).replace(/[^0-9.]/g,'');
	bb = ts_getInnerText(b.cells[SORT_COLUMN_INDEX]).replace(/[^0-9.]/g,'');
	return parseFloat(aa) - parseFloat(bb);
}

function ts_sort_numeric(a,b) { 
	aa = parseFloat(ts_getInnerText(a.cells[SORT_COLUMN_INDEX]));
	if (isNaN(aa)) {
		aa = 0;
	}
	bb = parseFloat(ts_getInnerText(b.cells[SORT_COLUMN_INDEX])); 
	if (isNaN(bb)) {
		bb = 0;
	}
	return aa-bb;
}

function ts_sort_caseinsensitive(a,b) {
	aa = ts_getInnerText(a.cells[SORT_COLUMN_INDEX]).toLowerCase();
	bb = ts_getInnerText(b.cells[SORT_COLUMN_INDEX]).toLowerCase();
	if (aa==bb) {
		return 0;
	}
	if (aa<bb) {
		return -1;
	}
	return 1;
}

function ts_sort_default(a,b) {
	aa = ts_getInnerText(a.cells[SORT_COLUMN_INDEX]);
	bb = ts_getInnerText(b.cells[SORT_COLUMN_INDEX]);
	if (aa==bb) {
		return 0;
	}
	if (aa<bb) {
		return -1;
	}
	return 1;
}

function addEvent(elm, evType, fn, useCapture)
// addEvent and removeEvent
// cross-browser event handling for IE5+,	NS6 and Mozilla
// By Scott Andrew
{
	if (elm.addEventListener){
		elm.addEventListener(evType, fn, false);
		return true;
	} else if (elm.attachEvent){
		var r = elm.attachEvent("on"+evType, fn);
		return r;
	} else {
		alert("Handler could not be set");
	}
} 

function replace(s, t, u) {
  /*
  **  Replace a token in a string
  **    s  string to be processed
  **    t  token to be found and removed
  **    u  token to be inserted
  **  returns new String
  */
  i = s.indexOf(t);
  r = "";
  if (i == -1) return s;
  r += s.substring(0,i) + u;
  if ( i + t.length < s.length)
    r += replace(s.substring(i + t.length, s.length), t, u);
  return r;
}

function alternate(table) {
	// Take object table and get all it's tbodies.
	var tableBodies = table.getElementsByTagName("tbody");
	// Loop through these tbodies
	for (var i = 0; i < tableBodies.length; i++) {
		// Take the tbody, and get all it's rows
		var tableRows = tableBodies[i].getElementsByTagName("tr");
		// Loop through these rows
		for (var j=0, j1 = 0; j < tableRows.length; j++, j1++) {
                
                        if(tableRows[j].className == 'totalCls') {
                            j1++;
//                            alert(j1);
                            continue;
                        } 
			// Check if j is even, and apply classes for both possible results
			if ( (j1 % 2) == 0  ) {
				if (tableRows[j].className == 'odd' || !(tableRows[j].className.indexOf('odd') == -1) ) {
					tableRows[j].className = replace(tableRows[j].className, 'odd', 'even');
				} else if (tableRows[j].className.indexOf('even') == -1 ) {
					tableRows[j].className += " even";
				}
			} else {
				if (tableRows[j].className == 'even' || !(tableRows[j].className.indexOf('even') == -1) ) {
					tableRows[j].className = replace(tableRows[j].className, 'even', 'odd');
				} else if (tableRows[j].className.indexOf('odd') == -1 ) {
				    tableRows[j].className += " odd";
				}
			} 
		}
	}
}


function createCookie(name,value,days) {
	if (days) {
		var date = new Date();
		date.setTime(date.getTime()+(days*24*60*60*1000));
		var expires = "; expires="+date.toGMTString();
	}
	else var expires = "";
	document.cookie = name+"="+value+expires+"; path=/";
}

function getCookie(name) {
	var nameEQ = name + "=";
	var ca = document.cookie.split(';');
	for(var i=0;i < ca.length;i++) {
		var c = ca[i];
		while (c.charAt(0)==' ') c = c.substring(1,c.length);
		if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
	}
	return null;
}

function eraseCookie(name) {
	createCookie(name,"",-1);
}
