/*
    steps for rust
    go to top level folder, in terminal do "cargo new project_folder_name"
    cd into the project_folder_name > there should be a folder called "src" with a "main.rs" file
    -- open that file, edit, save
    -- then in terminal "cargo run"
    -- to compile a binary, do "cargo build" ; this makes a binary in the target/debug folder
    -- to make a more optimized release build, do "cargo build --release"; this makes a binary in target/release folder

    -- to link other files, the functions have to be marked "pub"
    --  Then you add in the main.rs a section up top to import the module using "mod [name of other module without rs extension]"
    -- functions in other files don't need to be uppercase to be recognized

*/
mod test2;

fn main() {
    println!("Hello, world!");
    test();
    test2::test2p();
}

fn test() {
    println!("test test")
}
