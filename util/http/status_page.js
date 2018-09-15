function JsonToHTML(json_obj) {
	var str = '';
	Object.keys(json_obj).forEach(function (key) {
		value = json_obj[key];
		str += "<div style='margin-top:20px;'>" + span(key, 'title_text');
		if (!isObject(value)) {
			str += value_text(value);
		} else {
			str += objectObjectToHTML(value);
		}
		str += "</div><div class='separator'></div>";
	});

	return str;

	function objectObjectToHTML(objmap) {
		var s = '';
		Object.keys(objmap).forEach(function (key) {
			value = objmap[key];
			if (isObject(value)) {
				s += span(key + ':', 'key_text_bold')
				s += objectObjectToHTML(value);
			} else {
				s += key_text(key) + value_text(value);
			}
		});
		return s;
	}

	function isObject(o) {
		return Object.prototype.toString.call(o) === '[object Object]';
	}

	function span(t, s) { return "<span class='" + s + "'>" + t + "</span>";}

	function key_text(t) {
		return span(t + ':', 'key_text');
	}

	function value_text(t) {
		return span(t + ' ', 'value_text');
	}

}
