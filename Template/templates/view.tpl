{{template header .header}}

{{if .books}}
{{elseif .books}}
{{else}}
{{end}}

{{if data.books.length > 0}}
{{elseif data.books.length > 1}}
{{else}}
{{end}}

{{/* 商品数量 */}}
<div>book count: {{data.books.length}}</div>

{{/* 商品列表 */}}
<div>
	<table>
{{range .books}}
		<tr>
			<td>{{.id}}</td>
			<td>{{.name}}</td>
			<td>{{.price}}</td>
			<td>{{.cover}}</td>
		</tr>
{{endrange}}
	</table>
</div>

{{/* 商品列表 */}}
<div>
	<table>
{{range var i in data.books}}
{{#data = data.books[i]}}
		<tr>
			<td>{{.id}}</td>
			<td>{{.name}}</td>
			<td>{{.price}}</td>
			<td>{{.cover}}</td>
		</tr>
{{endrange}}
	</table>
</div>

{{template footer .footer}}
