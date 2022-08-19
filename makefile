all:
	python -m build -w -o ./dist

clean:
	rm -rf ./build
	rm -rf ./dist

upload_test: clean all
	twine upload --repository testpypi dist/*

upload: clean all
	twine upload dist/*
