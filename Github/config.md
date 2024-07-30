


%% to find the remote url %%
git config --get remote.origin.url

%%to select the file for push%%
git add .
git add file_path

%% status of file %%
git status


%% after creating a new repository%%
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/dftml/test.git
git push -u origin main


%% to the existing repository%%
git remote add origin https://github.com/dftml/test.git
git branch -M main
git push -u origin main