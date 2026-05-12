namespace TeamActivity.Judge;

public static class RunNames
{
    private static readonly string[] Names =
    [
        "John Wick", "The Bride", "Jack Bauer", "Ethan Hunt", "Ellen Ripley",
        "James Bond", "Jason Bourne", "Max Rockatansky", "Sarah Connor", "John McClane",
        "Indiana Jones", "Han Solo", "Furiosa", "Katniss Everdeen", "Beatrix Kiddo",
        "Tony Stark", "Natasha Romanoff", "Bryan Mills", "Frank Martin", "Leon",
        "Vincent Vega", "Jules Winnfield", "Tyler Durden", "Patrick Bateman", "Walter White",
        "Jesse Pinkman", "Don Draper", "Tony Soprano", "Omar Little", "Stringer Bell",
        "Frank Underwood", "Claire Underwood", "Cersei Lannister", "Tyrion Lannister", "Daenerys Targaryen",
        "Arya Stark", "Jon Snow", "Walter Hartwell", "Lalo Salamanca", "Mike Ehrmantraut",
        "Rust Cohle", "Marty Hart", "Jimmy McGill", "Chuck McGill", "Gus Fring",
        "The Mandalorian", "Ahsoka Tano", "Obi-Wan Kenobi", "Darth Vader", "Emperor Palpatine",
    ];

    public static string Random() => Names[System.Random.Shared.Next(Names.Length)];
}
