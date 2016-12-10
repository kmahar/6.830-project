from flask import Flask, render_template, request

app = Flask(__name__)

HASHTAG = 'test'

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/index_2')
def test():
	return render_template('index_2.html')

@app.route('/set_hashtag', methods=['POST'])
def set_hashtag():
	if 'hashtag' not in request.form:
		return 'Missing hashtag parameter', 400

	tag = request.form['hashtag']
	HASHTAG = tag
	return "Ok"
		
if __name__ == "__main__":
	app.run()