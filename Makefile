publish:
	pip install --target vendor -r requirements.txt
	cd vendor && zip -ur ../function.zip *
	zip -g function.zip callback.py stream.py utils/*

test:
	python -m unittest