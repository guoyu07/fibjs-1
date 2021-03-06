// copyright dotcoo@163.com

// template librarry
// author : dotcoo@163.com

var fs = require("fs");
var path = require("path");

function Template(source) {
	this.source = path.fullpath(source ? source : "templates");
};

Template.prototype.parse = function (js) {
	js = js.replace(/\\/g, 								"\\\\");
	js = js.replace(/'/g, 								"\\'");
	// js = js.replace(/^\s+/gm, 							"");
	// js = js.replace(/\s+$/gm, 							"");
	// js = js.replace(/\n/g, 								"");
	js = js.replace(/\t/g, 								"\\t");
	js = js.replace(/\n/g, 								"\\n");
	js = js.replace(/{{\/\*(.+?)\*\/}}/g, 				"");
	js = js.replace(/{{if \.(.+?)}}/g, 					"';if(data.$1){html+='");
	js = js.replace(/{{if (.+?)}}/g, 					"';if($1){html+='");
	js = js.replace(/{{else}}/g, 						"';}else{html+='");
	js = js.replace(/{{else ?if \.(.+?)}}/g, 			"';}else if(data.$1){html+='");
	js = js.replace(/{{else ?if (.+?)}}/g, 				"';}else if($1){html+='");
	js = js.replace(/{{end}}/g, 						"';}html+='");
	js = js.replace(/{{range \.(.+?)}}/g, 				"';for(var i in data.$1){data=data.$1[i];html+='");
	js = js.replace(/{{range (.+?)}}/g, 				"';for($1){html+='");
	js = js.replace(/{{endrange}}/g, 					"';data=_data;}html+='");
	js = js.replace(/{{template (\S+?)}}/g, 			"'+this.render('$1',data)+'");
	js = js.replace(/{{template (\S+?) \.(.+?)}}/g, 	"'+this.render('$1',data.$2)+'");
	js = js.replace(/{{template (\S+?) (.+?)}}/g, 		"'+this.render('$1',$2)+'");
	js = js.replace(/{{#(.+?)}}/g, 						"';$1;html+='");
	js = js.replace(/{{\.(.+?)}}/g, 					"'+data.$1+'");
	js = js.replace(/{{(.+?)}}/g, 						"'+$1+'");
	return js;
};

Template.prototype.build = function (name, source) {
	var content = fs.readFile(source).toString();
	var prefix = "var data=_data;var html='";
	var suffix = "';return html;";
	var js = prefix + this.parse(content) + suffix;
	this["tpls_" + name] = new Function("_data", js);
};

Template.prototype.builds = function () {
	var tpls = fs.readdir(this.source);
	for (var i = 0; i < tpls.length; i++) {
		var tpl = tpls[i];
		if (tpl.isDirectory() || path.extname(tpl.name) !== ".tpl") {
			continue;
		}
		var name = path.basename(tpl.name, ".tpl");
		var source = this.source + "/" + name + ".tpl";
		this.build(name, source);
	}
};

Template.prototype.render = function (name, _data) {
	if (!this["tpls_" + name]) {
		return 'template ' + name + ' not found!';
	}
	return this["tpls_" + name](_data);
};

exports.Template = Template;
